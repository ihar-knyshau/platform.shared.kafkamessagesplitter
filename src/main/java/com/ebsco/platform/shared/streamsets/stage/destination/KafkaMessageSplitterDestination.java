package com.ebsco.platform.shared.streamsets.stage.destination;

import com.ebsco.platform.shared.kafka.KafkaFactory;
import com.ebsco.platform.shared.kafka.KafkaMessageSplitter;
import com.ebsco.platform.shared.streamsets.Errors;
import com.ebsco.platform.shared.streamsets.Groups;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class KafkaMessageSplitterDestination extends BaseTarget {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSplitterDestination.class);

    public static final int TARGET_CONSUMER_MIN_ZERO_LAG_THRESHOLD = 1;

    Producer<byte[], byte[]> producer;
    KafkaMessageSplitter messageSplitter;
    KafkaSplitTopicConsumptionMonitor topicMonitor;

    public abstract Integer getChunkSize();

    public abstract String getBrokers();

    public abstract String getTopic();

    public abstract String getTargetConsumerGroupId();

    public abstract Integer getTargetConsumerCacheTtlMs();

    public abstract Integer getTargetConsumerZeroLagThreshold();

    public abstract Map<String, String> getKafkaProducerProperties();

    public abstract String getContentFieldName();

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        List<ConfigIssue> issues = super.init();
        String configName = "config";

        if (getBrokers().isEmpty()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.KafkaChunkedGroup.Kafka.name(), configName, Errors.CONFIG, "No brokers provided"
                    )
            );
        }
        if (getTopic().isEmpty()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.KafkaChunkedGroup.Kafka.name(), configName, Errors.CONFIG, "No topic provided"
                    )
            );
        }
        Properties props = new Properties();
        String bootstrapServers = getBrokers().toLowerCase();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);


        producer = KafkaFactory.createProducer(props);
        messageSplitter = new KafkaMessageSplitter(getChunkSize());

        topicMonitor = (KafkaSplitTopicConsumptionMonitor)Optional
                .ofNullable(getTargetConsumerGroupId())
                .map(StringUtils::trimToNull)
                .map(
                        targetConsumerGroupId ->
                                KafkaSplitTopicConsumptionMonitor
                                        .builder()
                                        .props(props)
                                        .bootstrapServers(bootstrapServers)
                                        .groupId(targetConsumerGroupId)
                                        .topic(getTopic())
                                        .accessCacheTtlMs(getTargetConsumerCacheTtlMs())
                                        .build()
                )
                .orElse(null);

        return issues;
    }

    /**
     * Writes a single record to the destination.
     *
     * @param record the record to write to the destination.
     * @throws OnRecordErrorException when a record cannot be written.
     */
    public void write(Record record) throws OnRecordErrorException {
        // wait until all the previous messages has been consumed (if configured)
        waitForTopicBeenConsumed(record);
        List<ProducerRecord<byte[], byte[]>> kafkaRecords = messageSplitter.createChunkRecords(messageSplitter.splitMessage(record.get("/"+getContentFieldName()).getValueAsString()), getTopic());
        kafkaRecords.forEach(kafkaRecord -> producer.send(kafkaRecord));
    }

    // it waits for topic been consumed
    // it stops the execution in case of errors or interruption
    void waitForTopicBeenConsumed(Record record) throws OnRecordErrorException {
        KafkaSplitTopicConsumptionMonitor monitor = this.topicMonitor;
        if (null == monitor) {
            return;
        }

        long zeroLagThreshold = Math.max(
                TARGET_CONSUMER_MIN_ZERO_LAG_THRESHOLD,
                Optional.ofNullable(getTargetConsumerZeroLagThreshold()).orElse(0)
        );

        try {
            while (true) {
                // obtain a lag from Kafka
                // XXX: it's required to do so before the waiting
                // XXX: there is the cache inside the monitor
                long lag = monitor.getLag();

                // check the lag if we're ready to go
                if (lag <= zeroLagThreshold) {
                    return;
                }

                // wait (and there is no use in waiting less than this value)
                monitor.waitCacheExpired();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OnRecordErrorException(record, Errors.IO, e.getMessage(), e);
        } catch (Exception e) {
            throw new OnRecordErrorException(record, Errors.IO, e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void write(Batch batch) throws StageException {
        Iterator<Record> batchIterator = batch.getRecords();

        while (batchIterator.hasNext()) {
            Record record = batchIterator.next();
            try {
                write(record);
            } catch (OnRecordErrorException e) {
                switch (getContext().getOnErrorRecord()) {
                    case DISCARD:
                        break;
                    case TO_ERROR:
                        getContext().toError(record, e.getErrorCode(), e.getMessage());
                        break;
                    case STOP_PIPELINE:
                        throw new StageException(Errors.STAGE, e.toString());
                    default:
                        throw new IllegalStateException(String.format("Error: %s", getContext().getOnErrorRecord().getLabel()));
                }
            }
        }
    }
}
