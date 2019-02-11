package com.ebsco.platform.shared;

import com.ebsco.platform.shared.kafka.KafkaMessageSender;
import com.ebsco.platform.shared.kafka.KafkaMessageSplitter;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PartsConsumer {
    private static final String chunkTopic = "test16";
    private static final String consumerGroup = "test16";
    private static final String kafkaUri = "quickstart.cloudera:9092";

    private Map<String, Queue<byte[]>> chunksMap = new HashMap<>();
    private Map<String, byte[]> completedMap = new HashMap<>();
    private Map<Integer, Long> offsetForPartitionMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        new PartsConsumer().consume();
    }

    private void consume() throws IOException {
        Instant instant = Instant.now();
        System.out.println(instant);
        List<String> files = IOUtils.readLines(PartsConsumer.class.getClassLoader().getResourceAsStream("asimov_txt"), Charsets.UTF_8);

        List<String> list = new ArrayList<>();
        Consumer<byte[], byte[]> consumer = KafkaMessageSender.createConsumer(kafkaUri, consumerGroup);
        consumer.subscribe(Collections.singletonList(chunkTopic));

        for (String file : files) {
            Producer<byte[], byte[]> producer = KafkaMessageSender.createProducer(kafkaUri);
            new Thread(() -> {
                InputStream inputStream = PartsConsumer.class.getResourceAsStream("/asimov_txt/" + file);
                String strMessage = null;
                try {
                    strMessage = IOUtils.toString(inputStream, UTF_8);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                KafkaMessageSplitter kafkaMessageSplitter = new KafkaMessageSplitter(1024);
                List<byte[]> chunks = kafkaMessageSplitter.splitMessage(strMessage);
                List<ProducerRecord<byte[], byte[]>> records = kafkaMessageSplitter.createChunkRecords(chunks, chunkTopic);

                for (ProducerRecord<byte[], byte[]> record : records) {
//                    try {
//                        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 10 + 1));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    producer.send(record);
                }
                list.add(strMessage);
            }).start();
        }

        System.out.println(Duration.between(instant, Instant.now()));

        for (int i = 0; i < 5; i++) {
            new Thread(() -> {
                while (true) {
//                    try {
//                        Thread.sleep(ThreadLocalRandom.current().nextInt(100, 5000 + 1));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMinutes(1));

                    Instant ins = Instant.now();

                    consumerRecords.records(chunkTopic).forEach(e -> {
                        if (e.headers().lastHeader(KafkaMessageSplitter.FINAL_CHUNK_KEY).value()[0] == (byte) 1) {
                            storeResult(e);
                        } else {
                            storeChunk(e);
                        }
                        offsetForPartitionMap.put(e.partition(), e.offset());
                        if (chunksMap.isEmpty()) {

                            checkFiles(list);

                            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                            offsetForPartitionMap.forEach((key, value) -> offsets.put(new TopicPartition(chunkTopic, key), new OffsetAndMetadata(value + 1)));
                            consumer.commitSync(offsets);
//                    completedMap.values().forEach(s -> System.out.println(new String(s)));
                            System.out.println("processed messages: " + completedMap.size());
                            completedMap.clear();
                        }
                    });
                    System.out.println("Ptocessing time:" + Duration.between(ins, Instant.now()));
                }
            }).start();
        }
    }

    private void storeChunk(ConsumerRecord<byte[], byte[]> e) {
        if (!chunksMap.containsKey(new String(e.key()))) {
            Queue<byte[]> queue = new LinkedList<>();
            queue.offer(e.value());
            chunksMap.put(new String(e.key()), queue);
        } else {
            chunksMap.get(new String(e.key())).offer(e.value());
        }
    }

    private void storeResult(ConsumerRecord<byte[], byte[]> e) {
        if (chunksMap.containsKey(new String(e.key()))) {
            Queue<byte[]> queue = chunksMap.getOrDefault(new String(e.key()), new LinkedList<>());
            queue.offer(e.value());
            completedMap.put(new String(e.key()), concatBytes(queue));
        } else {
            completedMap.put(new String(e.key()), e.value());
        }
        chunksMap.remove(new String(e.key()));
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

    private void checkFiles(List<String> list) {
        for (byte[] value : completedMap.values()) {
            if (!list.contains(new String(value))) {
                for (int i = 0; i < list.size(); i++) {
                    String s = list.get(i);
                    if (s.contains(new String(value))) {
                        throw new IllegalStateException("fail:" + new String(value));
                    }
                }
                throw new IllegalStateException("fail");
            }
        }
    }
}