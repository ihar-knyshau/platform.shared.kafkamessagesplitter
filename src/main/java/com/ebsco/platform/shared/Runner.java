package com.ebsco.platform.shared;

import com.ebsco.platform.shared.kafka.KafkaFactory;
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
    private static final String kafkaUri = "kafka.uri";

    public static void main(String[] args) throws IOException {
        InputStream inputStream = Runner.class.getResourceAsStream("/message");
        String strMessage = IOUtils.toString(inputStream, UTF_8);

        Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(kafkaUri, consumerGroup);
        consumer.subscribe(Collections.singletonList(chunkTopic));

        KafkaMessageSplitter kafkaMessageSplitter = new KafkaMessageSplitter(1024);
        List<byte[]> chunks = kafkaMessageSplitter.splitMessage(strMessage);
        List<ProducerRecord<byte[], byte[]>> records = kafkaMessageSplitter.createChunkRecords(chunks, chunkTopic);
        Producer<byte[], byte[]> producer = KafkaFactory.createProducer(kafkaUri);
        for (ProducerRecord<byte[], byte[]> record : records) {
            producer.send(record);
        }

        ConsumerRecords<byte[], byte[]> consumerRecords;
        do {
            consumerRecords = consumer.poll(Duration.ofMinutes(1));
        } while (consumerRecords.isEmpty());

        consumerRecords.records(chunkTopic).iterator().forEachRemaining(record -> {
            String composed = new String(record.value());
            System.out.println(strMessage.equals(composed));
        });

    }
}
