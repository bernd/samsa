package com.github.bernd.samsa;

public class LogConfigBuilder {
    public int segmentSize;
    public long segmentMs;
    public long segmentJitterMs;
    public long flushInterval;
    public long flushMs;
    public long retentionSize;
    public long retentionMs;
    public int maxMessageSize;
    public int maxIndexSize;
    public int indexInterval;
    public long fileDeleteDelayMs;
    public long deleteRetentionMs;
    public double minCleanableDirtyRatio;
    public boolean compact;
    public boolean uncleanLeaderElectionEnable;
    public int minInSyncReplicas;

    public LogConfigBuilder() {
        segmentSize = LogConfig.Defaults.SEGMENT_SIZE;
        segmentMs = LogConfig.Defaults.SEGMENT_MS;
        segmentJitterMs = LogConfig.Defaults.SEGMENT_JITTER_MS;
        flushInterval = LogConfig.Defaults.FLUSH_INTERVAL;
        flushMs = LogConfig.Defaults.FLUSH_MS;
        retentionSize = LogConfig.Defaults.RETENTION_SIZE;
        retentionMs = LogConfig.Defaults.RETENTION_MS;
        maxMessageSize = LogConfig.Defaults.MAX_MESSAGE_SIZE;
        maxIndexSize = LogConfig.Defaults.MAX_INDEX_SIZE;
        indexInterval = LogConfig.Defaults.INDEX_INTERVAL;
        fileDeleteDelayMs = LogConfig.Defaults.FILE_DELETE_DELAY_MS;
        deleteRetentionMs = LogConfig.Defaults.DELETE_RETENTION_MS;
        minCleanableDirtyRatio = LogConfig.Defaults.MIN_CLEANABLE_DIRTY_RATIO;
        compact = LogConfig.Defaults.COMPACT;
        uncleanLeaderElectionEnable = LogConfig.Defaults.UNCLEAN_LEADER_ELECTION_ENABLE;
        minInSyncReplicas = LogConfig.Defaults.MIN_IN_SYNC_REPLICAS;
    }

    public LogConfig build() {
        return new LogConfig(segmentSize,
                segmentMs,
                segmentJitterMs,
                flushInterval,
                flushMs,
                retentionSize,
                retentionMs,
                maxMessageSize,
                maxIndexSize,
                indexInterval,
                fileDeleteDelayMs,
                deleteRetentionMs,
                minCleanableDirtyRatio,
                compact,
                uncleanLeaderElectionEnable,
                minInSyncReplicas);
    }

    public LogConfigBuilder segmentSize(final int segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public LogConfigBuilder segmentMs(final long segmentMs) {
        this.segmentMs = segmentMs;
        return this;
    }

    public LogConfigBuilder segmentJitterMs(long segmentJitterMs) {
        this.segmentJitterMs = segmentJitterMs;
        return this;
    }

    public LogConfigBuilder flushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
        return this;
    }

    public LogConfigBuilder flushMs(long flushMs) {
        this.flushMs = flushMs;
        return this;
    }

    public LogConfigBuilder retentionSize(long retentionSize) {
        this.retentionSize = retentionSize;
        return this;
    }

    public LogConfigBuilder retentionMs(long retentionMs) {
        this.retentionMs = retentionMs;
        return this;
    }

    public LogConfigBuilder maxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
        return this;
    }

    public LogConfigBuilder maxIndexSize(int maxIndexSize) {
        this.maxIndexSize = maxIndexSize;
        return this;
    }

    public LogConfigBuilder indexInterval(int indexInterval) {
        this.indexInterval = indexInterval;
        return this;
    }

    public LogConfigBuilder fileDeleteDelayMs(long fileDeleteDelayMs) {
        this.fileDeleteDelayMs = fileDeleteDelayMs;
        return this;
    }

    public LogConfigBuilder deleteRetentionMs(long deleteRetentionMs) {
        this.deleteRetentionMs = deleteRetentionMs;
        return this;
    }

    public LogConfigBuilder minCleanableDirtyRatio(double minCleanableDirtyRatio) {
        this.minCleanableDirtyRatio = minCleanableDirtyRatio;
        return this;
    }

    public LogConfigBuilder compact(boolean compact) {
        this.compact = compact;
        return this;
    }

    public LogConfigBuilder uncleanLeaderElectionEnable(boolean uncleanLeaderElectionEnable) {
        this.uncleanLeaderElectionEnable = uncleanLeaderElectionEnable;
        return this;
    }

    public LogConfigBuilder minInSyncReplicas(int minInSyncReplicas) {
        this.minInSyncReplicas = minInSyncReplicas;
        return this;
    }
}
