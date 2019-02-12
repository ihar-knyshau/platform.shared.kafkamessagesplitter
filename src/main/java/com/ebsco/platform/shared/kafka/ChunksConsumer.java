package com.ebsco.platform.shared.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ChunksConsumer implements Consumer<byte[], byte[]> {

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    private Map<String, Map<String, Queue<byte[]>>> topicsChunks = new LinkedHashMap<>();


    public ChunksConsumer(Map<String, Object> configs) {
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumer = new KafkaConsumer(configs, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    public ChunksConsumer(Properties properties) {
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaConsumer = new KafkaConsumer(properties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private Optional<ConsumerRecord<byte[], byte[]>> storeChunk(Map<String, Queue<byte[]>> storage, ConsumerRecord<byte[], byte[]> record) {
        if (record.headers().lastHeader(KafkaMessageSplitter.FINAL_CHUNK_KEY).value()[0] == (byte) 1) {
            return storeResult(storage, record);
        } else {
            if (!storage.containsKey(new String(record.key()))) {
                Queue<byte[]> queue = new LinkedList<>();
                storage.put(new String(record.key()), queue);
            }
            storage.get(new String(record.key())).offer(record.value());
            return Optional.empty();
        }
    }


    private Optional<ConsumerRecord<byte[], byte[]>> storeConsumerRecord(ConsumerRecord<byte[], byte[]> record) {
        if (!topicsChunks.containsKey(record.topic())) {
            Map<String, Queue<byte[]>> topicRecords = new HashMap<>();
            topicsChunks.put(record.topic(), topicRecords);
        }
        Optional<ConsumerRecord<byte[], byte[]>> output = storeChunk(topicsChunks.get(record.topic()), record);
        if (topicsChunks.get(record.topic()).isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
            kafkaConsumer.commitSync(offsets);
        }
        return output;
    }

    private Optional<ConsumerRecord<byte[], byte[]>> storeResult(Map<String, Queue<byte[]>> storage, ConsumerRecord<byte[], byte[]> record) {
        byte[] message;
        int totalChunks = ByteBuffer.wrap(record.headers().lastHeader(KafkaMessageSplitter.TOTAL_CHUNKS).value()).getInt();
        Optional output;
        if (storage.containsKey(new String(record.key()))) {
            Queue<byte[]> queue = storage.get(new String(record.key()));
            queue.offer(record.value());
            if (queue.size() == totalChunks) {
                message = concatBytes(queue);
                output = Optional.of(new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), message));
            } else {
                output = Optional.empty();
            }
            //TODO:write log notifications
            storage.remove(new String(record.key()));
        } else {
            if (totalChunks == 1) {
                message = record.value();
                output = Optional.of(new ConsumerRecord(record.topic(), record.partition(), record.offset(), record.key(), message));
            } else {
                output = Optional.empty();
                //TODO:write log notifications
            }
        }
        return output;
    }

    private byte[] concatBytes(Queue<byte[]> bytes) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        while (!bytes.isEmpty()) {
            try {
                outputStream.write(bytes.poll());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return outputStream.toByteArray();
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
                .map(this::storeConsumerRecord)
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
        return 0;
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        return 0;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return null;
    }

    @Override
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaConsumer.partitionsFor(topic);
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return null;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return null;
    }

    @Override
    public Set<TopicPartition> paused() {
        return null;
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {

    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {

    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return null;
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return null;
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void close(long timeout, TimeUnit unit) {

    }

    @Override
    public void close(Duration timeout) {

    }

    @Override
    public void wakeup() {

    }
}