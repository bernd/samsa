package com.github.bernd.samsa;

import com.github.bernd.samsa.utils.Utils;

import java.util.Random;

/**
 * Configuration settings for a log
 */
public class LogConfig {
    private final int segmentSize;
    private final long segmentMs;
    private final long segmentJitterMs;
    private final long flushInterval;
    private final long flushMs;
    private final long retentionSize;
    private final long retentionMs;
    private final int maxMessageSize;
    private final int maxIndexSize;
    private final int indexInterval;
    private final long fileDeleteDelayMs;
    private final long deleteRetentionMs;
    private final double minCleanableDirtyRatio;
    private final boolean compact;
    private final boolean uncleanLeaderElectionEnable;
    private final int minInSyncReplicas;
    private long randomSegmentJitter;

    public static class Defaults {
        public static final int SEGMENT_SIZE = 1024 * 1024;
        public static final long SEGMENT_MS = Long.MAX_VALUE;
        public static final long SEGMENT_JITTER_MS = 0L;
        public static final long FLUSH_INTERVAL = Long.MAX_VALUE;
        public static final long FLUSH_MS = Long.MAX_VALUE;
        public static final long RETENTION_SIZE = Long.MAX_VALUE;
        public static final long RETENTION_MS = Long.MAX_VALUE;
        public static final int MAX_MESSAGE_SIZE = Integer.MAX_VALUE;
        public static final int MAX_INDEX_SIZE = 1024 * 1024;
        public static final int INDEX_INTERVAL = 4096;
        public static final long FILE_DELETE_DELAY_MS = 60 * 1000L;
        public static final long DELETE_RETENTION_MS = 24 * 60 * 60 * 1000L;
        public static final double MIN_CLEANABLE_DIRTY_RATIO = 0.5;
        public static final boolean COMPACT = false;
        public static final boolean UNCLEAN_LEADER_ELECTION_ENABLE = true;
        public static final int MIN_IN_SYNC_REPLICAS = 1;
    }

    public LogConfig(final int segmentSize,
                     final long SegmentMs,
                     final long segmentJitterMs,
                     final long flushInterval,
                     final long flushMs,
                     final long retentionSize,
                     final long retentionMs,
                     final int maxMessageSize,
                     final int maxIndexSize,
                     final int indexInterval,
                     final long fileDeleteDelayMs,
                     final long deleteRetentionMs,
                     final double minCleanableDirtyRatio,
                     final boolean compact,
                     final boolean uncleanLeaderElectionEnable,
                     final int minInSyncReplicas) {
        this.segmentSize = segmentSize;
        this.segmentMs = SegmentMs;
        this.segmentJitterMs = segmentJitterMs;
        this.flushInterval = flushInterval;
        this.flushMs = flushMs;
        this.retentionSize = retentionSize;
        this.retentionMs = retentionMs;
        this.maxMessageSize = maxMessageSize;
        this.maxIndexSize = maxIndexSize;
        this.indexInterval = indexInterval;
        this.fileDeleteDelayMs = fileDeleteDelayMs;
        this.deleteRetentionMs = deleteRetentionMs;
        this.minCleanableDirtyRatio = minCleanableDirtyRatio;
        this.compact = compact;
        this.uncleanLeaderElectionEnable = uncleanLeaderElectionEnable;
        this.minInSyncReplicas = minInSyncReplicas;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public long getSegmentMs() {
        return segmentMs;
    }

    public long getSegmentJitterMs() {
        return segmentJitterMs;
    }

    public long getFlushInterval() {
        return flushInterval;
    }

    public long getFlushMs() {
        return flushMs;
    }

    public long getRetentionSize() {
        return retentionSize;
    }

    public long getRetentionMs() {
        return retentionMs;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public int getMaxIndexSize() {
        return maxIndexSize;
    }

    public int getIndexInterval() {
        return indexInterval;
    }

    public long getFileDeleteDelayMs() {
        return fileDeleteDelayMs;
    }

    public long getDeleteRetentionMs() {
        return deleteRetentionMs;
    }

    public double getMinCleanableDirtyRatio() {
        return minCleanableDirtyRatio;
    }

    public boolean isCompact() {
        return compact;
    }

    public boolean isUncleanLeaderElectionEnable() {
        return uncleanLeaderElectionEnable;
    }

    public int getMinInSyncReplicas() {
        return minInSyncReplicas;
    }

    public long getRandomSegmentJitter() {
        if (segmentJitterMs == 0) {
            return 0;
        } else {
            return Utils.abs(new Random().nextInt()) % Math.min(segmentJitterMs, segmentMs);
        }
    }
}
