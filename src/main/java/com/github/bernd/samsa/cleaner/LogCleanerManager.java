package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.OffsetCheckpoint;
import com.github.bernd.samsa.TopicAndPartition;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manage the state of each partition being cleaned.
 * If a partition is to be cleaned, it enters the LogCleaningInProgress state.
 * While a partition is being cleaned, it can be requested to be aborted and paused. Then the partition first enters
 * the LogCleaningAborted state. Once the cleaning task is aborted, the partition enters the LogCleaningPaused state.
 * While a partition is in the LogCleaningPaused state, it won't be scheduled for cleaning again, until cleaning is
 * requested to be resumed.
 */
public class LogCleanerManager {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleanerManager.class);
    private final ConcurrentMap<TopicAndPartition, Log> logs;

    public enum State {
        LOG_CLEANING_IN_PROGRESS, LOG_CLEANING_ABORTED, LOG_CLEANING_PAUSED
    }

    /* the offset checkpoints holding the last cleaned point for each log */
    private final Map<File, OffsetCheckpoint> checkpoints = Maps.newHashMap();

    /* the set of logs currently being cleaned */
    private final Map<TopicAndPartition, State> inProgress = Maps.newHashMap();

    /* a global lock used to control all access to the in-progress set and the offset checkpoints */
    private final ReentrantLock lock = new ReentrantLock();

    /* for coordinating the pausing and the cleaning of a partition */
    private final Condition pausedCleaningCond;

    /* a gauge for tracking the cleanable ratio of the dirtiest log */
    private volatile double dirtiestLogCleanableRatio = 0.0;

    public LogCleanerManager(final List<File> logDirs,
                             final ConcurrentMap<TopicAndPartition, Log> logs) throws IOException {
        this.logs = logs;

        for (final File dir : logDirs) {
            checkpoints.put(dir, new OffsetCheckpoint(new File(dir, "cleaner-offset-checkpoint")));
        }

        this.pausedCleaningCond = lock.newCondition();
    }

    /**
     * @return the position processed for all logs.
     */
    public Map<TopicAndPartition, Long> allCleanerCheckpoints() throws IOException {
        final Map<TopicAndPartition, Long> map = Maps.newHashMap();

        for (OffsetCheckpoint checkpoint : checkpoints.values()) {
            map.putAll(checkpoint.read());
        }

        return map;
    }

    /**
     * Choose the log to clean next and add it to the in-progress set. We recompute this
     * every time off the full set of logs to allow logs to be dynamically added to the pool of logs
     * the log manager maintains.
     */
    public Optional<LogToClean> grabFilthiestLog() throws IOException {
        lock.lock();
        try {
            final Map<TopicAndPartition, Long> lastClean = allCleanerCheckpoints();
            final List<LogToClean> dirtyLogs = Lists.newArrayList();

            for (Map.Entry<TopicAndPartition, Log> entry : logs.entrySet()) {
                final TopicAndPartition topicAndPartition = entry.getKey();
                final Log log = entry.getValue();

                // skip any logs marked for delete rather than dedupe
                if (!log.getConfig().isCompact()) {
                    continue;
                }

                // skip any logs already in-progress
                if (inProgress.containsKey(topicAndPartition)) {
                    continue;
                }

                // create a LogToClean instance for each
                // if the log segments are abnormally truncated and hence the checkpointed offset
                // is no longer valid, reset to the log starting offset and log the error event
                final long logStartOffset = log.logSegments().iterator().next().getBaseOffset();
                final long firstDirtyOffset;
                final long offset = lastClean.containsKey(topicAndPartition) ? lastClean.get(topicAndPartition) : logStartOffset;

                if (offset < logStartOffset) {
                    LOG.error(String.format("Resetting first dirty offset to log start offset %d since the checkpointed offset %d is invalid.",
                            logStartOffset, offset));
                    firstDirtyOffset = logStartOffset;
                } else {
                    firstDirtyOffset = offset;
                }

                final LogToClean logToClean = new LogToClean(topicAndPartition, log, firstDirtyOffset);

                // skip any empty logs
                if (logToClean.totalBytes() > 0) {
                    dirtyLogs.add(logToClean);
                }
            }

            if (dirtyLogs.isEmpty()) {
                // TODO Check if LogToClean comparison works as expected here!
                dirtiestLogCleanableRatio = Ordering.natural().max(dirtyLogs).cleanableRatio();
            } else {
                dirtiestLogCleanableRatio = 0;
            }

            // and must meet the minimum threshold for dirty byte ratio
            final Iterable<LogToClean> cleanableLogs = Iterables.filter(dirtyLogs, new Predicate<LogToClean>() {
                @Override
                public boolean apply(LogToClean logToClean) {
                    return logToClean.cleanableRatio() > logToClean.getLog().getConfig().getMinCleanableDirtyRatio();
                }
            });

            if (Iterables.isEmpty(cleanableLogs)) {
                return Optional.absent();
            } else {
                final LogToClean filthiest = Ordering.natural().max(cleanableLogs);
                inProgress.put(filthiest.getTopicAndPartition(), State.LOG_CLEANING_IN_PROGRESS);
                return Optional.of(filthiest);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     * This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
     */
    public void abortCleaning(final TopicAndPartition topicAndPartition) throws InterruptedException {
        lock.lock();
        try {
            abortAndPauseCleaning(topicAndPartition);
            resumeCleaning(topicAndPartition);
            LOG.info(String.format("The cleaning for partition %s is aborted", topicAndPartition));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     * 1. If the partition is not in progress, mark it as paused.
     * 2. Otherwise, first mark the state of the partition as aborted.
     * 3. The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
     * throws a LogCleaningAbortedException to stop the cleaning task.
     * 4. When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.
     * 5. abortAndPauseCleaning() waits until the state of the partition is changed to paused.
     */
    public void abortAndPauseCleaning(final TopicAndPartition topicAndPartition) throws InterruptedException {
        lock.lock();
        try {
            final State state = inProgress.get(topicAndPartition);

            if (state == null) {
                inProgress.put(topicAndPartition, State.LOG_CLEANING_PAUSED);
            } else {
                switch (state) {
                    case LOG_CLEANING_IN_PROGRESS:
                        inProgress.put(topicAndPartition, State.LOG_CLEANING_ABORTED);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Compaction for partition %s cannot be aborted and paused since it is in %s state.",
                                topicAndPartition, state));
                }
            }
            while (!isCleaningInState(topicAndPartition, State.LOG_CLEANING_PAUSED)) {
                pausedCleaningCond.await(100, TimeUnit.MILLISECONDS);
            }

            LOG.info(String.format("The cleaning for partition %s is aborted and paused", topicAndPartition));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    public void resumeCleaning(final TopicAndPartition topicAndPartition) {
        lock.lock();
        try {
            final State state = inProgress.get(topicAndPartition);

            if (state == null) {
                throw new IllegalStateException(String.format("Compaction for partition %s cannot be resumed since it is not paused.",
                        topicAndPartition));
            } else {
                switch (state) {
                    case LOG_CLEANING_PAUSED:
                        inProgress.remove(topicAndPartition);
                        break;
                    default:
                        throw new IllegalStateException(String.format("Compaction for partition %s cannot be resumed since it is in %s state.",
                                topicAndPartition, state));

                }
            }
        } finally {
            lock.unlock();
        }
        LOG.info(String.format("Compaction for partition %s is resumed", topicAndPartition));
    }

    /**
     * Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
     */
    public boolean isCleaningInState(final TopicAndPartition topicAndPartition, final State expectedState) {
        final State state = inProgress.get(topicAndPartition);

        return state != null && state == expectedState;
    }

    /**
     * Check if the cleaning for a partition is aborted. If so, throw an exception.
     */
    public void checkCleaningAborted(final TopicAndPartition topicAndPartition) throws LogCleaningAbortedException {
        lock.lock();
        try {
            if (isCleaningInState(topicAndPartition, State.LOG_CLEANING_ABORTED)) {
                throw new LogCleaningAbortedException();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
     */
    public void doneCleaning(final TopicAndPartition topicAndPartition, final File dataDir, final long endOffset) throws IOException {
        lock.lock();
        try {
            final State state = inProgress.get(topicAndPartition);

            switch (state) {
                case LOG_CLEANING_IN_PROGRESS:
                    final OffsetCheckpoint checkpoint = checkpoints.get(dataDir);
                    final Map<TopicAndPartition, Long> offsets = checkpoint.read();
                    offsets.put(topicAndPartition, endOffset);
                    checkpoint.write(offsets);
                    inProgress.remove(topicAndPartition);
                    break;
                case LOG_CLEANING_ABORTED:
                    inProgress.put(topicAndPartition, State.LOG_CLEANING_PAUSED);
                    pausedCleaningCond.signalAll();
                    break;
                default:
                    throw new IllegalStateException(String.format("In-progress partition %s cannot be in %s state.", topicAndPartition, state));
            }
        } finally {
            lock.unlock();
        }
    }
}
