package com.ebsco.platform.shared;

import com.ebsco.platform.shared.kafka.KafkaMessageSender;
import com.ebsco.platform.shared.kafka.KafkaMessageSplitter;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static java.nio.charset.StandardCharsets.UTF_8;

public class PartsConsumer {
    private static final String chunkTopic = "test1";
    private static final String consumerGroup = "test1";
    private static final String kafkaUri = "quickstart.cloudera:9092";

    private Map<String, Queue<byte[]>> chunksMap = new HashMap<>();
    private Map<String, byte[]> completedMap = new HashMap<>();

    public static void main(String[] args) throws IOException {
        new PartsConsumer().consume();
    }

    private void consume() throws IOException {
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

        while (true){
            ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMinutes(1));
            consumerRecords.records(chunkTopic).forEach(e -> {
                if(e.headers().lastHeader(KafkaMessageSplitter.FINAL_CHUNK_KEY).value()[0] == (byte) 1){
                    storeResult(e);
                } else {
                    storeChunk(e);
                }
                if(chunksMap.isEmpty()){
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    offsets.put(new TopicPartition(chunkTopic, e.partition()), new OffsetAndMetadata(e.offset()));
                    consumer.commitSync(offsets);
                    completedMap.values().forEach(s -> System.out.println(new String(s)));
                }
            });

        }
    }

    private void storeChunk(ConsumerRecord<byte[], byte[]> e){
        if(!chunksMap.containsKey(new String(e.key()))){
            Queue<byte[]> queue = new LinkedList<>();
            queue.offer(e.value());
            chunksMap.put(new String(e.key()), queue);
        } else {
            chunksMap.get(new String(e.key())).offer(e.value());
        }
    }

    private void storeResult(ConsumerRecord<byte[], byte[]> e){
        if(chunksMap.containsKey(new String(e.key()))){
            Queue<byte[]> queue = chunksMap.getOrDefault(new String(e.key()), new LinkedList<>());
            queue.offer(e.value());
            completedMap.put(new String(e.key()), concatBytes(queue));
        } else {
            completedMap.put(new String(e.key()), e.value());
        }
        chunksMap.remove(new String(e.key()));
    }

    private byte[] concatBytes(Queue<byte[]> bytes){
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        while (!bytes.isEmpty()){
            try {
                outputStream.write(bytes.poll());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return outputStream.toByteArray( );
    }
}