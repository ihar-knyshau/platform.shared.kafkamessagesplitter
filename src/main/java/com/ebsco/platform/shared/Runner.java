package com.ebsco.platform.shared;

import com.ebsco.platform.shared.kafka.ChunksConsumer;
import com.ebsco.platform.shared.kafka.KafkaFactory;
import com.ebsco.platform.shared.kafka.KafkaMessageSplitter;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Runner {
    private static final String chunkTopic = "test35";
    private static final String consumerGroup = "test35";
    private static final String kafkaUri = "localhost:9092";
    int recordsCount = 0;
    int unsubscribed = 0;

    public static void main(String[] args) throws IOException {
        new Runner().consume();
    }

    private void consume() throws IOException {
      /*  Instant instant = Instant.now();
        System.out.println(instant);
        List<String> files = IOUtils.readLines(Objects.requireNonNull(Runner.class.getClassLoader().getResourceAsStream("asimov_txt")), Charsets.UTF_8);

//        Map<String, Integer> list = new HashMap<>();

        for (String file : files) {
            Producer<byte[], byte[]> producer = KafkaFactory.createProducer(kafkaUri);
//            new Thread(() -> {
                InputStream inputStream = Runner.class.getResourceAsStream("/asimov_txt/" + file);
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
                    producer.send(record);
                }
//                list.put(strMessage, 0);
//            }).start();
        }

        System.out.println(Duration.between(instant, Instant.now()));

//        for (int i = 0; i < 2; i++) {
//            new Thread(() -> {
//                Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(kafkaUri, consumerGroup);
//
//                consumer.subscribe(Collections.singletonList(chunkTopic), new ChunkRebalanceListener((ChunksConsumer) consumer));
//
//                ConsumerRecords<byte[], byte[]> consumerRecords;
//
//                do {
//                    if (recordsCount > 30 && unsubscribed < 1) {
//                        unsubscribed++;
//                        consumer.unsubscribe();
//                        System.out.println("UNSUBSCRIBE of " + consumer + "!!!111");
//                        break;
//                    } else {
//                        consumerRecords = consumer.poll(Duration.ofMinutes(1));
//                        recordsCount += consumerRecords.count();
//                        System.out.println("Records consumed by " + consumer + ": " + recordsCount + "/" + list.size());
//                        consumerRecords.records(chunkTopic).iterator().forEachRemaining(record -> {
//                            String composed = null;
//                            try {
//                                composed = IOUtils.toString(record.value(), "UTF-8");
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
//                            checkFiles(list, composed);
//                        });
//                    }
//                } while (list.values().stream().anyMatch(e -> e == 0));
//
//            }).start();
//        }
        for (int i = 0; i < 2; i++) {
            new Thread(() -> {
                Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(kafkaUri, consumerGroup);

                consumer.subscribe(Collections.singletonList(chunkTopic), new ChunkRebalanceListener((ChunksConsumer) consumer));

                ConsumerRecords<byte[], byte[]> consumerRecords;

                do {
                    if (recordsCount > 30 && unsubscribed < 1) {
                        unsubscribed++;
                        consumer.unsubscribe();
                        System.out.println("UNSUBSCRIBE of " + consumer + "!!!111");
                        break;
                    } else {
                        consumerRecords = consumer.poll(Duration.ofMinutes(1));
                        recordsCount += consumerRecords.count();
                        System.out.println("Records consumed by " + consumer + ": " + recordsCount + "/" + list.size());
                        consumerRecords.records(chunkTopic).iterator().forEachRemaining(record -> {
                            String composed = null;
                            try {
                                composed = IOUtils.toString(record.value(), "UTF-8");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            checkFiles(list, composed);
                        });
                    }
                } while (list.values().stream().anyMatch(e -> e == 0));

            }).start();
        }*/
    }

    private void checkFiles(Map<String, Integer> list, String composed) {
        if (!list.containsKey(composed)) {
            throw new IllegalStateException("fail");
        } else {
            list.put(composed, list.get(composed) + 1);
        }
    }

//    private void checkFiles(Map<String, Integer> list, String composed) {
//        if (!list.containsKey(composed)) {
//            throw new IllegalStateException("fail");
//        } else {
//            list.put(composed, list.get(composed) + 1);
//        }
//    }

//    class ChunkRebalanceListener implements ConsumerRebalanceListener {
//
//        private ChunksConsumer chunksConsumer;
//
//        public ChunkRebalanceListener(ChunksConsumer chunksConsumer) {
//            this.chunksConsumer = chunksConsumer;
//        }
//
//        @Override
//        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//        }
//
//        @Override
//        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
//            chunksConsumer.setNewCachePartitions(partitions);
//        }
//    }
}
