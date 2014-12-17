package com.github.bernd.samsa;

public class TopicAndPartition {
    private final String topic;
    private final int partition;

    public TopicAndPartition(final String topic, final int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public String toString() {
        return String.format("[%s,%s]", topic, partition);
    }
}
