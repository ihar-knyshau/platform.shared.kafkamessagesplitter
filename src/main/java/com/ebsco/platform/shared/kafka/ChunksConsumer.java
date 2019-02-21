package com.ebsco.platform.shared.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ChunksConsumer implements Consumer<byte[], byte[]> {
    public static final String CACHE_LIFESPAN_PROPERTY = "consumer.cache.lifespan";

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private KafkaTimeBasedChunkCache timeBasedChunkCache;
    private Map<TopicPartition, Long> possibleOffsets = new HashMap<>();
    private final Long DEFAULT_CACHE_LIFESPAN = 1000 * 60 * 3L;//3 mins


    public ChunksConsumer(Map<String, Object> configs) {
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Long cacheLifespan = (Long) configs.getOrDefault(CACHE_LIFESPAN_PROPERTY, DEFAULT_CACHE_LIFESPAN);
        timeBasedChunkCache = new KafkaTimeBasedChunkCache(cacheLifespan);
        kafkaConsumer = new KafkaConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public ChunksConsumer(Properties properties) {
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Optional lifespan = Optional.ofNullable(properties.getProperty(CACHE_LIFESPAN_PROPERTY));
        Long cacheLifespan = lifespan.isPresent() ? Long.parseLong((String) lifespan.get()) : DEFAULT_CACHE_LIFESPAN;
        timeBasedChunkCache = new KafkaTimeBasedChunkCache(cacheLifespan);
        kafkaConsumer = new KafkaConsumer(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }


    private void commitOffsetsIfPossible(Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        records.keySet().stream().forEach(topicPartition -> {
            //Get the maximal offset from completed\outdated records
            ConsumerRecord<byte[], byte[]> record = records.get(topicPartition).stream().max(Comparator.comparingLong(ConsumerRecord::offset)).get();
            //We need only to increase offset
            if (!possibleOffsets.containsKey(topicPartition) || possibleOffsets.get(topicPartition) < record.offset()) {
                possibleOffsets.put(topicPartition, record.offset() + 1);
            }
            //Check if we can commit the new offset
            if (timeBasedChunkCache.isTopicPartitionEmpty(topicPartition)) {
                if (!possibleOffsets.containsKey(topicPartition) || possibleOffsets.get(topicPartition) <= record.offset() + 1) {
                    offsets.put(topicPartition, new OffsetAndMetadata(record.offset() + 1));
                } else {
                    //This execution branch is reachable in case we have outdated records as input and we need to commit the offset of the last constructed message
                    offsets.put(topicPartition, new OffsetAndMetadata(possibleOffsets.get(topicPartition)));
                }
            }
        });
        if (!offsets.isEmpty()) {
            kafkaConsumer.commitSync(offsets);
        }
    }

    public void resetCache() {
        timeBasedChunkCache.cleanCache();
        possibleOffsets.clear();
    }

    public void setNewCachePartitions(Collection<TopicPartition> topicPartitions) {
        timeBasedChunkCache.setNewPartitions(topicPartitions);
        topicPartitions.forEach(topicPartition -> possibleOffsets.remove(topicPartition));

    }

    @Override
    public Set<TopicPartition> assignment() {
        return kafkaConsumer.assignment();
    }

    @Override
    public Set<String> subscription() {
        return kafkaConsumer.subscription();
    }

    @Override
    public void subscribe(Collection<String> topics) {
        kafkaConsumer.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(topics, callback);
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        kafkaConsumer.assign(partitions);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        kafkaConsumer.subscribe(pattern, callback);
    }

    @Override
    public void subscribe(Pattern pattern) {
        kafkaConsumer.subscribe(pattern);
    }

    @Override
    public void unsubscribe() {
        kafkaConsumer.unsubscribe();
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(long timeout) {
        return poll(Duration.ofMillis(timeout));
    }

    @Override
    public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
        List<ConsumerRecord> records = new ArrayList<>();
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> output = new HashMap<>();
        kafkaConsumer.poll(timeout).forEach(record -> records.add(record));
        records.stream()
                .map(record -> timeBasedChunkCache.put(record))
                .filter(consumerRecord -> ((Optional) consumerRecord).isPresent())
                .map(consumerRecord -> ((Optional) consumerRecord).get())
                .forEach(record -> {
                    ConsumerRecord<byte[], byte[]> castedRecord = (ConsumerRecord) record;
                    TopicPartition topicPartition = new TopicPartition(castedRecord.topic(), castedRecord.partition());
                    if (!output.containsKey(topicPartition)) {
                        output.put(topicPartition, new ArrayList<>());
                    }
                    output.get(topicPartition).add(castedRecord);
                });
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsToCheck = new HashMap<>();
        recordsToCheck.putAll(timeBasedChunkCache.removeOutdated(System.currentTimeMillis()));
        recordsToCheck.putAll(output);
        commitOffsetsIfPossible(recordsToCheck);
        return new ConsumerRecords<>(output);

    }

    @Override
    public void commitSync() {

    }

    @Override
    public void commitSync(Duration timeout) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {

    }

    @Override
    public void commitAsync() {

    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {

    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {

    }

    @Override
    public void seek(TopicPartition partition, long offset) {

    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {

    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {

    }

    @Override
    public long position(TopicPartition partition) {
        return kafkaConsumer.position(partition);
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.position(partition, timeout);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return kafkaConsumer.committed(partition);
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return kafkaConsumer.committed(partition, timeout);
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return kafkaConsumer.metrics();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return kafkaConsumer.partitionsFor(topic, timeout);
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return kafkaConsumer.listTopics();
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return kafkaConsumer.listTopics(timeout);
    }

    @Override
    public Set<TopicPartition> paused() {
        return kafkaConsumer.paused();
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        kafkaConsumer.pause(partitions);
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch);
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.beginningOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.beginningOffsets(partitions, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return kafkaConsumer.endOffsets(partitions);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return kafkaConsumer.endOffsets(partitions, timeout);
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        kafkaConsumer.close(timeout, unit);
    }

    @Override
    public void close(Duration timeout) {
        kafkaConsumer.close(timeout);
    }

    @Override
    public void wakeup() {
        kafkaConsumer.wakeup();
    }
}