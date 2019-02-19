package com.ebsco.platform.shared.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaTimeBasedChunkCache {
    private Map<TopicPartitionKey, Queue<ConsumerRecord<byte[], byte[]>>> topicsChunks = new HashMap<>();
    private Map<TopicPartitionKey, Long> keyLifespans = new HashMap<>();
    private Set<TopicPartitionKey> keysToIgnore = new HashSet<>();
    private Long cacheLifespan;

    public KafkaTimeBasedChunkCache(Long cacheTimeout) {
        cacheLifespan = cacheTimeout;
    }

    public Optional<ConsumerRecord<byte[], byte[]>> put(ConsumerRecord<byte[], byte[]> record) {
        TopicPartitionKey key = new TopicPartitionKey(record.topic(), record.partition(), new String(record.key()));
        int currentChunk = ByteBuffer.wrap(record.headers().lastHeader(KafkaMessageSplitter.CURRENT_CHUNK).value()).getInt();
        int totalChunks = ByteBuffer.wrap(record.headers().lastHeader(KafkaMessageSplitter.TOTAL_CHUNKS).value()).getInt();
        return storeChunk(key, record, currentChunk, totalChunks);
    }

    private Optional<ConsumerRecord<byte[], byte[]>> storeChunk(TopicPartitionKey key, ConsumerRecord<byte[], byte[]> record, int number, int total) {
        //In case we have 1 chunk only, no need to put in cache
        if (number == total && total == 1) {
            return Optional.of(record);
        }

        //In case we get the last chunk of a sequence we ignore, we need to remove the key from list of ignored keys
        if (number == total && keysToIgnore.contains(key)) {
            keysToIgnore.remove(key);
            return Optional.ofNullable(null);
        }

        //If we picked up a chunk from message body, we need to ignore it,as the head hasn't been transferred
        if (number > 1 && !topicsChunks.containsKey(key)) {
            keysToIgnore.add(key);
            return Optional.ofNullable(null);
        }
        //If we picked up last chunk, we need to construct message and return it
        if (number == total) {
            Queue<ConsumerRecord<byte[], byte[]>> chunks = topicsChunks.remove(key);
            keyLifespans.remove(key);
            chunks.offer(record);
            if (chunks.size() == total) {
                return storeResult(chunks, record);
            } else {
                return Optional.ofNullable(null);
            }
        }

        //If we picked up message head, we need to put it in cache
        if (!keysToIgnore.contains(key)) {
            if (!topicsChunks.containsKey(key)) {
                topicsChunks.put(key, new LinkedList<>());
            }
            topicsChunks.get(key).offer(record);
        }
        return Optional.ofNullable(null);
    }

    private Optional<ConsumerRecord<byte[], byte[]>> storeResult(Queue<ConsumerRecord<byte[], byte[]>> chunks, ConsumerRecord<byte[], byte[]> lastChunk) {
        List<byte[]> bytes = chunks.stream().map(record -> record.value()).collect(Collectors.toList());
        byte[] newValue = concatBytes(new LinkedList<>(bytes));
        return Optional.of(new ConsumerRecord<>(lastChunk.topic(), lastChunk.partition(), lastChunk.offset(), lastChunk.key(), newValue));
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

    public boolean isTopicPartitionEmpty(TopicPartition topicPartition) {
        return !topicsChunks.keySet().stream().anyMatch(key -> key.getTopicPartition().equals(topicPartition));
    }

    public Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> removeOutdated(Long timestamp) {
        List<TopicPartitionKey> recordsToRemove = keyLifespans.entrySet().stream()
                .filter(entry -> timestamp - entry.getValue() > cacheLifespan)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        recordsToRemove.forEach(key -> keyLifespans.remove(key));
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> output = new HashMap<>();
        recordsToRemove.stream().forEach(key -> {
            if (!output.containsKey(key.getTopicPartition())) {
                output.put(key.getTopicPartition(), new ArrayList<>());
            }

            output.get(key.getTopicPartition()).addAll(topicsChunks.remove(key));
        });
        return output;
    }

    public void cleanCache() {
        topicsChunks.clear();
        keysToIgnore.clear();
        keyLifespans.clear();
    }

    public void setNewPartitions(Collection<TopicPartition> topicPartitions) {
        List<TopicPartitionKey> keysToDelete = topicsChunks.keySet().stream().filter(key -> !topicPartitions.contains(key.getTopicPartition())).collect(Collectors.toList());
        keysToDelete.forEach(key -> {
            topicsChunks.remove(key);
            keysToIgnore.remove(key);
            keyLifespans.remove(key);
        });
    }
}
