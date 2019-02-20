package com.ebsco.platform.shared.kafka;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class KafkaTimeBasedChunkCacheTest {
    private final KafkaMessageSplitter messageSplitter = new KafkaMessageSplitter(1024);

    private List<ConsumerRecord<byte[], byte[]>> splitMessage(String topic, int partition, String message) {
        byte[] uuid = UUID.randomUUID().toString().getBytes(UTF_8);
        int[] counter = new int[]{0};
        List<byte[]> chunks = messageSplitter.splitMessage(message);
        return chunks.stream().map(chunk -> {
            counter[0]++;
            return generateConsumerRecord(topic, partition, counter[0], uuid, chunk, counter[0], chunks.size());
        }).collect(Collectors.toList());
    }


    private ConsumerRecord<byte[], byte[]> generateConsumerRecord(String topic, int partition, long offset, byte[] key, byte[] message, int postition, int total) {
        Iterable<Header> headers = messageSplitter.createChunkHeaders(key, postition, total, postition == total);
        return new ConsumerRecord(topic, partition, offset, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                -1L, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, key, message, new RecordHeaders(headers));

    }

    @Test
    public void testCacheState() throws IOException {
        String topicName = "testTopic";
        byte[] message = "Some message".getBytes();
        int partition = 0;

        List<ConsumerRecord<byte[], byte[]>> testData = Arrays.asList(
                generateConsumerRecord(topicName, partition, 0, "key1".getBytes(), message, 1, 2),
                generateConsumerRecord(topicName, partition, 0, "key2".getBytes(), message, 1, 3)
        );

        KafkaTimeBasedChunkCache cache = new KafkaTimeBasedChunkCache(1000 * 60 * 3L);
        assertTrue(cache.isTopicPartitionEmpty(new TopicPartition(topicName, partition)));
        testData.forEach(record -> cache.put(record));
        assertFalse(cache.isTopicPartitionEmpty(new TopicPartition(topicName, partition)));
        cache.cleanCache();
        assertTrue(cache.isTopicPartitionEmpty(new TopicPartition(topicName, partition)));
    }

    @Test
    public void testCacheConstructsMessageSuccessfully() throws IOException {
        String testMessage = IOUtils.toString(KafkaTimeBasedChunkCacheTest.class.getResourceAsStream("/message"), UTF_8);
        List<ConsumerRecord<byte[], byte[]>> testData = splitMessage("testTopic", 0, testMessage);
        ConsumerRecord<byte[], byte[]> lastChunk = testData.remove(testData.size() - 1);
        KafkaTimeBasedChunkCache cache = new KafkaTimeBasedChunkCache(1000 * 60 * 3L);
        testData.forEach(record ->
                assertFalse(cache.put(record).isPresent())
        );
        ConsumerRecord<byte[], byte[]> result = cache.put(lastChunk).get();
        assertEquals(testMessage, new String(result.value()));
        assertTrue(cache.isTopicPartitionEmpty(new TopicPartition("testTopic", 0)));
    }

    @Test
    public void testCanRemoveOutdated() throws IOException, InterruptedException {
        KafkaTimeBasedChunkCache cache = new KafkaTimeBasedChunkCache(1000 * 60 * 1L);
        String testMessage = IOUtils.toString(KafkaTimeBasedChunkCacheTest.class.getResourceAsStream("/message"), UTF_8);
        List<ConsumerRecord<byte[], byte[]>> testData = splitMessage("testTopic", 0, testMessage);
        testData.remove(testData.size() - 1);
        testData.forEach(record->cache.put(record));
        cache.removeOutdated(System.currentTimeMillis()+1000 * 60 * 1L + 1);
        assertTrue(cache.isTopicPartitionEmpty(new TopicPartition("testTopic", 0)));
    }
}
