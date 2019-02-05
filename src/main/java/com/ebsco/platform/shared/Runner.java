package com.ebsco.platform.shared;

import com.ebsco.platform.shared.kafka.KafkaMessageSender;
import com.ebsco.platform.shared.kafka.KafkaMessageSplitter;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Runner {
    private static final String chunkTopic = "finalchunk";
    private static final String consumerGroup = "finalchunk";
    private static final String kafkaUri = "finalchunk";

    public static void main(String[] args) throws IOException {
        InputStream inputStream = Runner.class.getResourceAsStream("/message");
        String strMessage = IOUtils.toString(inputStream, UTF_8);

        Consumer<byte[], byte[]> consumer = KafkaMessageSender.createConsumer(kafkaUri, consumerGroup);
        consumer.subscribe(Collections.singletonList(chunkTopic));

        KafkaMessageSplitter kafkaMessageSplitter = new KafkaMessageSplitter(1024);
        List<byte[]> chunks = kafkaMessageSplitter.splitMessage(strMessage);
        List<ProducerRecord<byte[], byte[]>> records = kafkaMessageSplitter.createChunkRecords(chunks, chunkTopic);
        Producer<byte[], byte[]> producer = KafkaMessageSender.createProducer(kafkaUri);
        for (ProducerRecord<byte[], byte[]> record : records) {
            producer.send(record);
        }

        ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMinutes(1));
        consumerRecords.records(chunkTopic).iterator().forEachRemaining(System.out::println);
    }
}
