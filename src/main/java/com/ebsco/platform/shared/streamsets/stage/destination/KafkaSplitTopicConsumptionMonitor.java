package com.ebsco.platform.shared.streamsets.stage.destination;


import com.ebsco.platform.shared.kafka.KafkaFactory;
import lombok.Builder;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

@NotThreadSafe
public class KafkaSplitTopicConsumptionMonitor {

    public static final long CACHE_MIN_TTL_MS = TimeUnit.SECONDS.toMillis(1);

    private final Properties properties;
    private final String topic;
    private final long accessCacheTtlMs;

    private long accessCacheDeadlineTimestamp;
    private long accessCacheLag;

    @Builder
    private KafkaSplitTopicConsumptionMonitor(
            Properties props,
            String bootstrapServers,
            String groupId,
            String topic,
            Number accessCacheTtlMs
    ) {
        this.accessCacheTtlMs = Math.max(
                CACHE_MIN_TTL_MS,
                Optional.ofNullable(accessCacheTtlMs).orElse(0L).longValue()
        );
        this.topic = topic;
        this.properties = createProperties(props, bootstrapServers, groupId);
    }

    /**
     * Requests and return the lag for consumer group for specified topic.
     * This method uses internal cache (it won't call Kafka until the cache expires).
     *
     * @return the lag for consumer group for specified topic
     * @throws CancellationException
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public long getLag() throws InterruptedException, ExecutionException, TimeoutException {
        // first, look up the cache (return if it's still fresh)
        long timestamp = System.currentTimeMillis();
        if (timestamp <= accessCacheDeadlineTimestamp) {
            return accessCacheLag;
        }

        // then make a real call to Kafka (it will take time)
        Long lag = requestLag().get(this.accessCacheTtlMs, TimeUnit.MILLISECONDS);
        long current = System.currentTimeMillis();

        // update cache and return the result
        accessCacheDeadlineTimestamp = current + accessCacheTtlMs;
        accessCacheLag = lag;
        return lag;
    }

    /**
     * It requests (always) Kafka asynchronously to collect the lag for consumer group for specified topic.
     */
    @Nonnull
    public Future<Long> requestLag() {
        try (AdminClient adminClient = createKafkaAdminClient()) {
            return adminClient
                    .describeTopics(Collections.singleton(this.topic))
                    .values().get(this.topic)
                    .thenApply(new KafkaFuture.Function<TopicDescription, Long>() {
                        @Override
                        public Long apply(TopicDescription topicDescription) {
                            return requestTopicLag(topicDescription);
                        }
                    });
        }
    }

    /**
     * it waits/sleep until the cached data expires
     */
    public void waitCacheExpired() throws InterruptedException {
        long timeout = Math.max(1, accessCacheDeadlineTimestamp - System.currentTimeMillis());
        Thread.sleep(timeout); // it's required to call 'sleep' (even if the timeout <= 0) to handle possible interruption
    }

    @Nonnull
    protected long requestTopicLag(TopicDescription topicDescription) {
        List<TopicPartition> topicPartitions = topicDescription
                .partitions()
                .stream()
                .map(p -> new TopicPartition(this.topic, p.partition()))
                .collect(Collectors.toList());

        try (Consumer<byte[], byte[]> consumer = createKafkaConsumer()) {

            // request latest offsets
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            // assign to topics and read current position
            consumer.assign(topicPartitions);
            return topicPartitions.stream()
                    .mapToLong(p -> Math.max(0, endOffsets.getOrDefault(p, 0L) - consumer.position(p)))
                    .sum();
        }
    }

    @Nonnull
    protected Consumer<byte[], byte[]> createKafkaConsumer() {
        return KafkaFactory.createConsumer(properties); //NOSONAR
    }

    @Nonnull
    protected AdminClient createKafkaAdminClient() {
        return AdminClient.create(this.properties); //NOSONAR
    }

    @Nonnull
    private static Properties createProperties(Properties props, String bootstrapServers, String groupId) {
        Properties properties = new Properties(props);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }

    private LongConsumer createNoopLongConsumer() {
        return new MutableLong()::setValue; // this construction required for SONARQUBE (it doesn't like empty methods)
    }

}