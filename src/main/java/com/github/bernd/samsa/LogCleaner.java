package com.github.bernd.samsa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the dedupe retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * <p/>
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The active log segment is always excluded from cleaning.
 * <p/>
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "dedupe" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 * <p/>
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See kafka.log.OffsetMap for details of
 * the implementation of the mapping.
 * <p/>
 * Once the key=>offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * <p/>
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * <p/>
 * Cleaned segments are swapped into the log as they become available.
 * <p/>
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * <p/>
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 */
public class LogCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    private final CleanerConfig config;
    private final List<File> logDirs;
    private final ConcurrentMap<TopicAndPartition, Log> logs;

    /**
     * @param config  Configuration parameters for the cleaner
     * @param logDirs The directories where offset checkpoints reside
     * @param logs    The pool of logs
     */
    public LogCleaner(final CleanerConfig config,
                      final List<File> logDirs,
                      final ConcurrentMap<TopicAndPartition, Log> logs) {
        this.config = config;
        this.logDirs = logDirs;
        this.logs = logs;
    }

    /**
     * Start the background cleaning
     */
    public void startup() {
        LOG.info("Starting the log cleaner");
        //cleaners.foreach(_.start())
    }

    /**
     * Stop the background cleaning
     */
    public void shutdown() {
        LOG.info("Shutting down the log cleaner.");
        //cleaners.foreach(_.shutdown())
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     */
    public void abortCleaning(final TopicAndPartition topicAndPartition) {
        //cleanerManager.abortCleaning(topicAndPartition);
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     */
    public void abortAndPauseCleaning(final TopicAndPartition topicAndPartition) {
        //cleanerManager.abortAndPauseCleaning(topicAndPartition);
    }

    /**
     * Resume the cleaning of a paused partition. This call blocks until the cleaning of a partition is resumed.
     */
    public void resumeCleaning(final TopicAndPartition topicAndPartition) {
        //cleanerManager.resumeCleaning(topicAndPartition);
    }

    // TODO Implement all methods!
}
