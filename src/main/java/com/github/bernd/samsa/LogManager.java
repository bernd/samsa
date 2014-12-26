package com.github.bernd.samsa;

import com.github.bernd.samsa.cleaner.CleanerConfig;
import com.github.bernd.samsa.cleaner.LogCleaner;
import com.github.bernd.samsa.utils.FileLock;
import com.github.bernd.samsa.utils.Scheduler;
import com.github.bernd.samsa.utils.Time;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * <p/>
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * <p/>
 * A background thread handles log retention by periodically truncating excess log segments.
 * <p/>
 * This class is thread-safe!
 */
public class LogManager {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    public static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";
    public static final String LOCK_FILE = ".lock";
    public static final long INITIAL_TASK_DELAY_MS = 30 * 1000L;

    private final Object logCreationOrDeletionLock = new Object();
    private final ConcurrentMap<TopicAndPartition, Log> logs = new ConcurrentHashMap<>();
    private final ConcurrentMap<File, OffsetCheckpoint> recoveryPointCheckpoints = new ConcurrentHashMap<>();
    private final LogCleaner cleaner;
    private final List<File> logDirs;
    private final Map<String, LogConfig> topicConfigs;
    private final LogConfig defaultConfig;
    private final CleanerConfig cleanerConfig;
    private final int ioThreads;
    private final long flushCheckMs;
    private final long flushCheckpointMs;
    private final long retentionCheckMs;
    private final Scheduler scheduler;
    private final BrokerState brokerState;
    private final Time time;
    private final List<FileLock> dirLocks;

    public LogManager(final List<File> logDirs,
                      final Map<String, LogConfig> topicConfigs,
                      final LogConfig defaultConfig,
                      final CleanerConfig cleanerConfig,
                      final int ioThreads,
                      final long flushCheckMs,
                      final long flushCheckpointMs,
                      final long retentionCheckMs,
                      final Scheduler scheduler,
                      final BrokerState brokerState,
                      final Time time) throws Throwable {
        this.logDirs = logDirs;
        this.topicConfigs = topicConfigs;
        this.defaultConfig = defaultConfig;
        this.cleanerConfig = cleanerConfig;
        this.ioThreads = ioThreads;
        this.flushCheckMs = flushCheckMs;
        this.flushCheckpointMs = flushCheckpointMs;
        this.retentionCheckMs = retentionCheckMs;
        this.scheduler = scheduler;
        this.brokerState = brokerState;
        this.time = time;

        createAndValidateLogDirs(logDirs);

        this.dirLocks = lockLogDirs(logDirs);

        for (File dir : logDirs) {
            recoveryPointCheckpoints.put(dir, new OffsetCheckpoint(new File(dir, RECOVERY_POINT_CHECKPOINT_FILE)));
        }

        loadLogs();

        if (cleanerConfig.isEnableCleaner()) {
            cleaner = new LogCleaner(cleanerConfig, logDirs, logs, time);
        } else {
            cleaner = null;
        }
    }

    /**
     * Create and check validity of the given directories, specifically:
     * <ol>
     * <li> Ensure that there are no duplicates in the directory list
     * <li> Create each directory if it doesn't exist
     * <li> Check that each path is a readable directory
     * </ol>
     */
    private void createAndValidateLogDirs(final List<File> dirs) {
        final Iterable<String> dirNames = Iterables.transform(dirs, new Function<File, String>() {
            @Override
            public String apply(final File file) {
                try {
                    return file.getCanonicalPath();
                } catch (IOException e) {
                    LOG.error(e.getMessage(), e);
                    return null;
                }
            }
        });

        if (Sets.newHashSet(dirNames).size() < dirs.size()) {
            throw new SamsaException("Duplicate log directory found: " + dirs.toString());
        }
        for (final File dir : dirs) {
            if (!dir.exists()) {
                LOG.info("Log directory '" + dir.getAbsolutePath() + "' not found, creating it.");
                final boolean created = dir.mkdirs();
                if (!created) {
                    throw new SamsaException("Failed to create data directory " + dir.getAbsolutePath());
                }
            }
            if (!dir.isDirectory() || !dir.canRead()) {
                throw new SamsaException(dir.getAbsolutePath() + " is not a readable log directory.");
            }
        }
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(List<File> dirs) throws IOException {
        final List<FileLock> locks = Lists.newArrayList();

        for (final File dir : dirs) {
            final FileLock lock = new FileLock(new File(dir, LOCK_FILE));
            if (!lock.tryLock()) {
                throw new SamsaException("Failed to acquire lock on file .lock in " + lock.getFile().getParentFile().getAbsolutePath() +
                        ". A Kafka instance in another process or thread is using this directory.");
            }
            locks.add(lock);
        }

        return locks;
    }

    /**
     * Recover and load all logs in the given data directories
     */
    private void loadLogs() throws Throwable {
        LOG.info("Loading logs.");

        final List<ExecutorService> threadPools = Lists.newArrayList();
        final Map<File, List<Future<?>>> jobs = Maps.newHashMap();

        for (final File dir : logDirs) {
            final ExecutorService pool = Executors.newFixedThreadPool(ioThreads);
            threadPools.add(pool);

            final File cleanShutdownFile = new File(dir, Log.CLEAN_SHUTDOWN_FILE);

            if (cleanShutdownFile.exists()) {
                LOG.debug(
                        "Found clean shutdown file. " +
                                "Skipping recovery for all logs in data directory: " +
                                dir.getAbsolutePath());
            } else {
                // log recovery itself is being performed by `Log` class during initialization
                brokerState.newState(BrokerState.States.RECOVERING_FROM_UNCLEAN_SHUTDOWN);
            }

            final Map<TopicAndPartition, Long> recoveryPoints = recoveryPointCheckpoints.get(dir).read();
            final List<Runnable> jobsForDir = Lists.newArrayList();

            for (final File logDir : dir.listFiles()) {
                if (!logDir.isDirectory()) {
                    continue;
                }

                jobsForDir.add(new Runnable() {
                    @Override
                    public void run() {
                        LOG.debug("Loading log '" + logDir.getName() + "'");

                        final TopicAndPartition topicPartition = Log.parseTopicPartitionName(logDir);
                        final LogConfig config = topicConfigs.containsKey(topicPartition.getTopic()) ? topicConfigs.get(topicPartition.getTopic()) : defaultConfig;
                        final Long logRecoveryPoint = recoveryPoints.containsKey(topicPartition) ? recoveryPoints.get(topicPartition) : 0L;

                        try {
                            final Log current = new Log(logDir, config, logRecoveryPoint, scheduler, time);
                            final Log previous = logs.put(topicPartition, current);

                            if (previous != null) {
                                throw new IllegalArgumentException(
                                        String.format("Duplicate log directories found: %s, %s!",
                                                current.getDir().getAbsolutePath(), previous.getDir().getAbsolutePath()));
                            }
                        } catch (IOException e) {
                            LOG.error("Log creation failed!", e);
                            throw new SamsaException("Log creation failed!", e);
                        }
                    }
                });
            }

            final List<Future<?>> jobResults = Lists.newArrayList();
            for (final Runnable job : jobsForDir) {
                jobResults.add(pool.submit(job));
            }

            jobs.put(cleanShutdownFile, jobResults);
        }


        try {
            for (Map.Entry<File, List<Future<?>>> entry : jobs.entrySet()) {
                final File cleanShutdownFile = entry.getKey();
                final List<Future<?>> jobResults = entry.getValue();

                for (Future<?> result : jobResults) {
                    result.get();
                }

                cleanShutdownFile.delete();
            }
        } catch (ExecutionException e) {
            LOG.error("There was an error in one of the threads during logs loading: " + e.getCause());
            throw e.getCause();
        } finally {
            for (ExecutorService pool : threadPools) {
                pool.shutdown();
            }
        }

        LOG.info("Logs loading complete.");
    }

    /**
     * Start the background threads to flush logs and do log cleanup
     */
    public void startup() {
    /* Schedule the cleanup task to delete old logs */
        if (scheduler != null) {
            LOG.info(String.format("Starting log cleanup with a period of %d ms.", retentionCheckMs));
            scheduler.schedule("samsa-log-retention",
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                cleanupLogs();
                            } catch (IOException | SamsaStorageException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    },
                    INITIAL_TASK_DELAY_MS,
                    retentionCheckMs,
                    TimeUnit.MILLISECONDS);
            LOG.info(String.format("Starting log flusher with a default period of %d ms.", flushCheckMs));
            scheduler.schedule("samsa-log-flusher",
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                flushDirtyLogs();
                            } catch (IOException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    },
                    INITIAL_TASK_DELAY_MS,
                    flushCheckMs,
                    TimeUnit.MILLISECONDS);
            scheduler.schedule("samsa-recovery-point-checkpoint",
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                checkpointRecoveryPointOffsets();
                            } catch (IOException e) {
                                LOG.error(e.getMessage(), e);
                            }
                        }
                    },
                    INITIAL_TASK_DELAY_MS,
                    flushCheckpointMs,
                    TimeUnit.MILLISECONDS);
        }

        if (cleanerConfig.isEnableCleaner()) {
            cleaner.startup();
        }
    }

    /**
     * Close all the logs
     */
    public void shutdown() throws Throwable {
        LOG.info("Shutting down.");

        final List<ExecutorService> threadPools = Lists.newArrayList();
        final Map<File, List<Future<?>>> jobs = Maps.newHashMap();

        // stop the cleaner first
        if (cleaner != null) {
            //Utils.swallow(cleaner.shutdown())
            try {
                cleaner.shutdown();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        // close logs in each dir
        for (final File dir : logDirs) {
            LOG.debug("Flushing and closing logs at " + dir);

            final ExecutorService pool = Executors.newFixedThreadPool(ioThreads);
            threadPools.add(pool);

            final Collection<Log> logsInDir = (logsByDir().containsKey(dir.toString()) ? logsByDir().get(dir.toString()) : Maps.<TopicAndPartition, Log>newHashMap()).values();
            final List<Runnable> jobsForDir = Lists.newArrayList();

            for (final Log log : logsInDir) {
                jobsForDir.add(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // flush the log to ensure latest possible recovery point
                            log.flush();
                        } catch (IOException e) {
                            LOG.error("Log flushing failed!", e);
                            throw new SamsaException("Log flushing failed!", e);
                        }
                        log.close();
                    }
                });
            }

            final List<Future<?>> jobResults = Lists.newArrayList();
            for (final Runnable job : jobsForDir) {
                jobResults.add(pool.submit(job));
            }

            jobs.put(dir, jobResults);
        }

        try {
            for (Map.Entry<File, List<Future<?>>> entry : jobs.entrySet()) {
                final File dir = entry.getKey();
                final List<Future<?>> jobResults = entry.getValue();

                for (Future<?> result : jobResults) {
                    result.get();
                }

                // update the last flush point
                LOG.debug("Updating recovery points at " + dir);
                checkpointLogsInDir(dir);

                // mark that the shutdown was clean by creating marker file
                LOG.debug("Writing clean shutdown marker at " + dir);
                // Was written as: Utils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
                try {
                    new File(dir, Log.CLEAN_SHUTDOWN_FILE).createNewFile();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        } catch (ExecutionException e) {
            LOG.error("There was an error in one of the threads during LogManager shutdown: " + e.getCause());
            throw e.getCause();
        } finally {
            for (ExecutorService pool : threadPools) {
                pool.shutdown();
            }
            // regardless of whether the close succeeded, we need to unlock the data directories
            for (final FileLock lock : dirLocks) {
                lock.destroy();
            }
        }

        LOG.info("Shutdown complete.");
    }

    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionAndOffsets Partition logs that need to be truncated
     */
    public void truncateTo(final Map<TopicAndPartition, Long> partitionAndOffsets) throws IOException, SamsaStorageException, InterruptedException {
        for (Map.Entry<TopicAndPartition, Long> entry : partitionAndOffsets.entrySet()) {
            final TopicAndPartition topicAndPartition = entry.getKey();
            final Long truncateOffset = entry.getValue();

            final Log log = logs.get(topicAndPartition);

            // If the log does not exist, skip it
            if (log != null) {
                //May need to abort and pause the cleaning of the log, and resume after truncation is done.
                final boolean needToStopCleaner = truncateOffset < log.activeSegment().getBaseOffset();
                if (needToStopCleaner && cleaner != null) {
                    cleaner.abortAndPauseCleaning(topicAndPartition);
                }
                log.truncateTo(truncateOffset);
                if (needToStopCleaner && cleaner != null) {
                    cleaner.resumeCleaning(topicAndPartition);
                }
            }
        }
        checkpointRecoveryPointOffsets();
    }

    /**
     * Delete all data in a partition and start the log at the new offset
     *
     * @param newOffset The new offset to start the log with
     */
    public void truncateFullyAndStartAt(final TopicAndPartition topicAndPartition, final long newOffset) throws IOException, SamsaStorageException, InterruptedException {
        final Log log = logs.get(topicAndPartition);
        // If the log does not exist, skip it
        if (log != null) {
            //Abort and pause the cleaning of the log, and resume after truncation is done.
            if (cleaner != null) {
                cleaner.abortAndPauseCleaning(topicAndPartition);
            }
            log.truncateFullyAndStartAt(newOffset);
            if (cleaner != null) {
                cleaner.resumeCleaning(topicAndPartition);
            }
        }
        checkpointRecoveryPointOffsets();
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     */
    public void checkpointRecoveryPointOffsets() throws IOException {
        for (final File dir : logDirs) {
            checkpointLogsInDir(dir);
        }
    }

    /**
     * Make a checkpoint for all logs in provided directory.
     */
    private void checkpointLogsInDir(final File dir) throws IOException {
        final Map<TopicAndPartition, Log> recoveryPoints = logsByDir().get(dir.toString());
        if (recoveryPoints != null) {
            final Map<TopicAndPartition, Long> values = Maps.transformValues(recoveryPoints, new Function<Log, Long>() {
                @Override
                public Long apply(Log log) {
                    return log.getRecoveryPoint();
                }
            });
            recoveryPointCheckpoints.get(dir).write(values);
        }
    }

    /**
     * Get the log if it exists, otherwise return None
     */
    public Optional<Log> getLog(final TopicAndPartition topicAndPartition) {
        final Log log = logs.get(topicAndPartition);
        if (log == null) {
            return Optional.absent();
        } else {
            return Optional.of(log);
        }
    }

    /**
     * Create a log for the given topic and the given partition
     * If the log already exists, just return a copy of the existing log
     */
    public Log createLog(final TopicAndPartition topicAndPartition, final LogConfig config) throws IOException {
        synchronized (logCreationOrDeletionLock) {
            final Log log = logs.get(topicAndPartition);

            // check if the log has already been created in another thread
            if (log != null) {
                return log;
            }

            // if not, create it
            final File dataDir = nextLogDir();
            final File dir = new File(dataDir, topicAndPartition.getTopic() + "-" + topicAndPartition.getPartition());
            dir.mkdirs();
            final Log newLog = new Log(dir, config, 0L, scheduler, time);
            logs.put(topicAndPartition, newLog);
            LOG.info(String.format("Created log for partition [%s,%d] in %s with properties {%s}.",
                    topicAndPartition.getTopic(),
                    topicAndPartition.getPartition(),
                    dataDir.getAbsolutePath(),
                    config.toString()));
            return newLog;
        }
    }

    /**
     * Delete a log.
     */
    public void deleteLog(final TopicAndPartition topicAndPartition) throws SamsaStorageException, InterruptedException {
        Log removedLog = null;
        synchronized (logCreationOrDeletionLock) {
            removedLog = logs.remove(topicAndPartition);
        }
        if (removedLog != null) {
            //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
            if (cleaner != null) {
                cleaner.abortCleaning(topicAndPartition);
            }
            removedLog.delete();
            LOG.info(String.format("Deleted log for partition [%s,%d] in %s.",
                    topicAndPartition.getTopic(),
                    topicAndPartition.getPartition(),
                    removedLog.getDir().getAbsolutePath()));
        }
    }

    /**
     * Choose the next directory in which to create a log. Currently this is done
     * by calculating the number of partitions in each directory and then choosing the
     * data directory with the fewest partitions.
     */
    private File nextLogDir() {
        if (logDirs.size() == 1) {
            return logDirs.get(0);
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories)
            //
            // this was the scala code:
            //
            //     val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
            //     val dirCounts = logDirs.map(dir => (dir.getPath, 0)).toMap
            //     var dirCounts = (dirCounts ++ logCounts).toBuffer
            //     // choose the directory with the least logs in it
            //     val leastLoaded = dirCounts.sortBy(_._2).head
            //     new File(leastLoaded._1)
            final Map<String, Long> dirCounts = Maps.newHashMap();

            for (final File dir : logDirs) {
                dirCounts.put(dir.getPath(), 0L);
            }

            for (final Log log : allLogs()) {
                dirCounts.put(log.getDir().getParent(), log.size());
            }

            // choose the directory with the least logs in it
            final Ordering<Map.Entry<String, Long>> ordering = new Ordering<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> left, Map.Entry<String, Long> right) {
                    return left.getValue().compareTo(right.getValue());
                }
            };

            final List<Map.Entry<String, Long>> entries = Lists.newArrayList(dirCounts.entrySet());

            // Sorts the entry sets by their value.
            Collections.sort(entries, ordering);

            // Take the first directory which should be the one with the least logs in it.
            return new File(entries.get(0).getKey());
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     */
    private int cleanupExpiredSegments(final Log log) throws IOException, SamsaStorageException {
        final long startMs = time.milliseconds();
        return log.deleteOldSegments(new Function<LogSegment, Boolean>() {
            @Override
            public Boolean apply(LogSegment segment) {
                return (startMs - segment.lastModified()) > log.getConfig().getRetentionMs();
            }
        });
    }

    /**
     * Runs through the log removing segments until the size of the log
     * is at least logRetentionSize bytes in size
     */
    private int cleanupSegmentsToMaintainSize(final Log log) throws IOException, SamsaStorageException {
        if (log.getConfig().getRetentionSize() < 0 || log.size() < log.getConfig().getRetentionSize()) {
            return 0;
        }
        final AtomicLong diff = new AtomicLong(log.size() - log.getConfig().getRetentionSize());
        return log.deleteOldSegments(new Function<LogSegment, Boolean>() {
            @Override
            public Boolean apply(final LogSegment segment) {
                if (diff.get() - segment.size() >= 0) {
                    // TODO: This is not really safe I guess...
                    diff.set(diff.get() - segment.size());
                    return true;
                } else {
                    return false;
                }
            }
        });
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     */
    public void cleanupLogs() throws IOException, SamsaStorageException {
        LOG.debug("Beginning log cleanup...");
        int total = 0;
        final long startMs = time.milliseconds();
        for (final Log log : allLogs()) {
            if (log.getConfig().isCompact()) {
                continue;
            }
            LOG.debug("Garbage collecting '" + log.name() + "'");
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        LOG.debug("Log cleanup completed. " + total + " files deleted in " +
                (time.milliseconds() - startMs) / 1000 + " seconds");
    }

    /**
     * Get all the partition logs
     */
    public Collection<Log> allLogs() {
        return logs.values();
    }

    /**
     * Get a map of TopicAndPartition => Log
     */
    public Map<TopicAndPartition, Log> logsByTopicPartition() {
        return Collections.unmodifiableMap(logs);
    }

    /**
     * Map of log dir to logs by topic and partitions in that dir
     */
    public Map<String, Map<TopicAndPartition, Log>> logsByDir() {
        final ConcurrentMap<String, Map<TopicAndPartition, Log>> byDir = Maps.newConcurrentMap();

        for (Map.Entry<TopicAndPartition, Log> entry : logsByTopicPartition().entrySet()) {
            final TopicAndPartition topicAndPartition = entry.getKey();
            final Log log = entry.getValue();

            byDir.putIfAbsent(log.getDir().getParent(), Maps.<TopicAndPartition, Log>newHashMap());
            byDir.get(log.getDir().getParent()).put(topicAndPartition, log);
        }

        return byDir;
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private void flushDirtyLogs() throws IOException {
        LOG.debug("Checking for dirty logs to flush...");

        for (Map.Entry<TopicAndPartition, Log> entry : logs.entrySet()) {
            final TopicAndPartition topicAndPartition = entry.getKey();
            final Log log = entry.getValue();

            try {
                final long timeSinceLastFlush = time.milliseconds() - log.lastFlushTime();
                LOG.debug("Checking if flush is needed on " + topicAndPartition.getTopic() + " flush interval  " + log.getConfig().getFlushMs() +
                        " last flushed " + log.lastFlushTime() + " time since last flush: " + timeSinceLastFlush);
                if (timeSinceLastFlush >= log.getConfig().getFlushMs()) {
                    log.flush();
                }
            } catch (Throwable e) {
                LOG.error("Error flushing topic " + topicAndPartition.getTopic(), e);
            }
        }
    }

    public List<File> getLogDirs() {
        return logDirs;
    }
}
