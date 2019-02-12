package com.ebsco.platform.shared;

public class GeneralIntegrationTest {

   /* private String KAFKA_URI;
    private String TOPIC_NAME;
    private String CONSUMER_GROUP;
    private Integer chunkSize;

    private Map<String, Queue<byte[]>> chunksMap = new HashMap<>();
    private Map<String, byte[]> completedMap = new HashMap<>();


    private void consume() throws IOException {
        try {
            loadProperties();
            InputStream messageContentStream = Runner.class.getResourceAsStream("/message");
            String strMessage = IOUtils.toString(messageContentStream, UTF_8);

            Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(KAFKA_URI, CONSUMER_GROUP);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            KafkaMessageSplitter kafkaMessageSplitter = new KafkaMessageSplitter(chunkSize);
            List<byte[]> chunks = kafkaMessageSplitter.splitMessage(strMessage);
            List<ProducerRecord<byte[], byte[]>> records = kafkaMessageSplitter.createChunkRecords(chunks, TOPIC_NAME);
            Producer<byte[], byte[]> producer = KafkaFactory.createProducer(KAFKA_URI);
            for (ProducerRecord<byte[], byte[]> record : records) {
                producer.send(record);
            }

            while (true) {
                ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMinutes(1));
                consumerRecords.records(TOPIC_NAME).forEach(e -> {
                    if (e.headers().lastHeader(KafkaMessageSplitter.FINAL_CHUNK_KEY).value()[0] == (byte) 1) {
                        storeResult(e);
                    } else {
                        storeChunk(e);
                    }
                    if (chunksMap.isEmpty()) {
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                        offsets.put(new TopicPartition(TOPIC_NAME, e.partition()), new OffsetAndMetadata(e.offset()));
                        consumer.commitSync(offsets);
                        completedMap.values().forEach(s -> System.out.println(new String(s)));
                    }
                });

            }
        } catch (Exception e) {

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

    private void loadProperties() throws IOException {
        InputStream configStream = Runner.class.getResourceAsStream("/config.properties");
        Properties config = new Properties();
        config.load(configStream);
        KAFKA_URI = config.getProperty("kafka.uri");
        TOPIC_NAME = config.getProperty("topic.name");
        CONSUMER_GROUP = config.getProperty("consumergroup.name");
        chunkSize = Integer.parseInt(config.getProperty("chunk.size"));
    }*/

}
