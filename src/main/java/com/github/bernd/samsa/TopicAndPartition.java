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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicAndPartition)) return false;

        TopicAndPartition that = (TopicAndPartition) o;

        if (partition != that.partition) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = topic != null ? topic.hashCode() : 0;
        result = 31 * result + partition;
        return result;
    }

    @Override
    public String toString() {
        return String.format("[%s,%s]", topic, partition);
    }
}
