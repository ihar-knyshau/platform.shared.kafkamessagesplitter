package com.ebsco.platform.shared.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

public class TopicPartitionOffset {
    private TopicPartition topicPartition;
    private long offset;

    public TopicPartitionOffset(String topic, int partition, long offset) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.offset = offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartitionOffset)) return false;
        TopicPartitionOffset that = (TopicPartitionOffset) o;
        return Objects.equals(topicPartition, that.getTopicPartition()) &&
                Objects.equals(offset, that.getOffset());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition.topic(), topicPartition.partition(), offset);
    }

    public String getTopic() {
        return topicPartition.topic();
    }

    public int getPartition() {
        return topicPartition.partition();
    }

    public long getOffset() {
        return offset;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
