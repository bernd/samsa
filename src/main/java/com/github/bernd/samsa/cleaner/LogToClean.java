package com.github.bernd.samsa.cleaner;

import com.github.bernd.samsa.Log;
import com.github.bernd.samsa.LogSegment;
import com.github.bernd.samsa.TopicAndPartition;

public class LogToClean implements Comparable<LogToClean> {
    private final TopicAndPartition topicAndPartition;
    private final Log log;
    private final long firstDirtyOffset;

    public LogToClean(final TopicAndPartition topicAndPartition,
                      final Log log,
                      final long firstDirtyOffset) {
        this.topicAndPartition = topicAndPartition;
        this.log = log;
        this.firstDirtyOffset = firstDirtyOffset;
    }

    public long cleanBytes() {
        long cleanBytes = 0;

        for (LogSegment segment : log.logSegments(-1, firstDirtyOffset - 1)) {
            cleanBytes += segment.size();
        }

        return cleanBytes;
    }

    public long dirtyBytes() {
        long dirtyBytes = 0;

        for (LogSegment segment : log.logSegments(firstDirtyOffset, Math.max(firstDirtyOffset, log.activeSegment().getBaseOffset()))) {
            dirtyBytes += segment.size();
        }

        return dirtyBytes;
    }

    public double cleanableRatio() {
        return dirtyBytes() / (double) totalBytes();
    }

    public long totalBytes() {
        return cleanBytes() + dirtyBytes();
    }

    @Override
    public int compareTo(LogToClean that) {
        return (int) Math.signum(cleanableRatio() - that.cleanableRatio());
    }

    public Log getLog() {
        return log;
    }

    public TopicAndPartition getTopicAndPartition() {
        return topicAndPartition;
    }

    public long getFirstDirtyOffset() {
        return firstDirtyOffset;
    }
}
