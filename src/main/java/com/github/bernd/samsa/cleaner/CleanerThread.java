package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.OffsetMapException;
import com.github.bernd.samsa.SamsaStorageException;
import com.github.bernd.samsa.SkimpyOffsetMap;
import com.github.bernd.samsa.TopicAndPartition;
import com.github.bernd.samsa.utils.ShutdownableThread;
import com.github.bernd.samsa.utils.SystemTime;
import com.github.bernd.samsa.utils.Throttler;
import com.github.bernd.samsa.utils.Time;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
 * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
 */
public class CleanerThread extends ShutdownableThread {
    private static final Logger LOG = LoggerFactory.getLogger(CleanerThread.class);

    private final Cleaner cleaner;
    private final CleanerConfig config;
    private final LogCleanerManager cleanerManager;
    private final AtomicBoolean isRunning;
    private volatile CleanerStats lastStats;
    private final CountDownLatch backOffWaitLatch = new CountDownLatch(1);

    public CleanerThread(final int threadId,
                         final CleanerConfig config,
                         final LogCleanerManager cleanerManager,
                         final Throttler throttler) throws NoSuchAlgorithmException {
        super("samsa-log-cleaner-thread-" + threadId, false);
        this.config = config;
        this.cleanerManager = cleanerManager;
        this.isRunning = getIsRunning();

        final Time time = new SystemTime();

        if (config.getDedupeBufferSize() / config.getNumThreads() > Integer.MAX_VALUE) {
            LOG.warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");
        }

        this.cleaner = new Cleaner(threadId,
                new SkimpyOffsetMap((int) Math.min(config.getDedupeBufferSize() / config.getNumThreads(), Integer.MAX_VALUE),
                        config.getHashAlgorithm()),
                config.getIoBuffersize() / config.getNumThreads() / 2,
                config.getMaxMessageSize(),
                config.getDedupeBufferLoadFactor(),
                throttler,
                time,
                new CheckDoneCallback<TopicAndPartition>() {
                    @Override
                    public void call(TopicAndPartition topicAndPartition) throws LogCleaningAbortedException {
                        checkDone(topicAndPartition);
                    }
                });

        this.lastStats = new CleanerStats(time);
    }

    private void checkDone(final TopicAndPartition topicAndPartition) throws LogCleaningAbortedException {
        if (!isRunning.get()) {
            throw new ThreadShutdownException();
        }
        cleanerManager.checkCleaningAborted(topicAndPartition);
    }

    /**
     * The main loop for the cleaner thread
     */
    @Override
    public void doWork() {
        try {
            cleanOrSleep();
        } catch (IOException | SamsaStorageException | OffsetMapException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void shutdown() {
        initiateShutdown();
        backOffWaitLatch.countDown();
        awaitShutdown();
    }

    /**
     * Clean a log if there is a dirty log available, otherwise sleep for a bit
     */
    private void cleanOrSleep() throws IOException, SamsaStorageException, OffsetMapException {
        final Optional<LogToClean> logToClean = cleanerManager.grabFilthiestLog();
        LogToClean cleanable = null;
        long endOffset = 0L;

        try {
            if (logToClean.isPresent()) {
                // there's a log, clean it
                cleanable = logToClean.get();
                endOffset = cleanable.getFirstDirtyOffset();
                try {
                    endOffset = cleaner.clean(cleanable);
                    recordStats(cleaner.getId(), cleanable.getLog().name(), cleanable.getFirstDirtyOffset(), endOffset, cleaner.getStats());
                } catch (LogCleaningAbortedException e) {
                    // task can be aborted, let it go.
                }
            } else {
                // there are no cleanable logs, sleep a while
                Uninterruptibles.awaitUninterruptibly(backOffWaitLatch, config.getBackOffMs(), TimeUnit.MILLISECONDS);
            }
        } finally {
            if (cleanable != null) {
                cleanerManager.doneCleaning(cleanable.getTopicAndPartition(), cleanable.getLog().getDir().getParentFile(), endOffset);
            }
        }
    }

    /**
     * Log out statistics on a single run of the cleaner.
     */
    private void recordStats(final int id,
                             final String name,
                             final long from,
                             final long to,
                             final CleanerStats stats) {
        lastStats = stats;
        cleaner.swapStats();
        final String message =
                String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n", id, name, from, to) +
                        String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n", mb(stats.bytesRead),
                                stats.elapsedSecs(),
                                mb(stats.bytesRead / stats.elapsedSecs())) +
                        String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.mapBytesRead),
                                stats.elapsedIndexSecs(),
                                mb(stats.mapBytesRead) / stats.elapsedIndexSecs(),
                                100 * stats.elapsedIndexSecs() / stats.elapsedSecs()) +
                        String.format("\tBuffer utilization: %.1f%%%n", 100 * stats.bufferUtilization) +
                        String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.bytesRead),
                                stats.elapsedSecs() - stats.elapsedIndexSecs(),
                                mb(stats.bytesRead) / (stats.elapsedSecs() - stats.elapsedIndexSecs()), 100 * stats.elapsedSecs() - stats.elapsedIndexSecs() / stats.elapsedSecs()) +
                        String.format("\tStart size: %,.1f MB (%,d messages)%n", mb(stats.bytesRead), stats.messagesRead) +
                        String.format("\tEnd size: %,.1f MB (%,d messages)%n", mb(stats.bytesWritten), stats.messagesWritten) +
                        String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n", 100.0 * (1.0 - ((double) stats.bytesWritten) / stats.bytesRead),
                                100.0 * (1.0 - ((double) stats.messagesWritten) / stats.messagesRead));
        LOG.info(message);
    }

    private double mb(final double bytes) {
        return bytes / (1024 * 1024);
    }
}
