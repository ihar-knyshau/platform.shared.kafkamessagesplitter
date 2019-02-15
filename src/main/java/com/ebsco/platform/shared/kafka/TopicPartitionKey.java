package com.ebsco.platform.shared.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

public class TopicPartitionKey {
    private TopicPartition topicPartition;
    private String key;

    public TopicPartitionKey(String topic, int partition, String key) {
        this.topicPartition = new TopicPartition(topic, partition);
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartitionKey)) return false;
        TopicPartitionKey that = (TopicPartitionKey) o;
        return Objects.equals(topicPartition, that.getTopicPartition()) &&
                Objects.equals(key, that.getKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicPartition.topic(), topicPartition.partition(), key);
    }
    @Override
    public String toString(){
        return topicPartition.toString()+" "+key;
    }

    public String getTopic() {
        return topicPartition.topic();
    }

    public int getPartition() {
        return topicPartition.partition();
    }

    public String getKey() {
        return key;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }
}
