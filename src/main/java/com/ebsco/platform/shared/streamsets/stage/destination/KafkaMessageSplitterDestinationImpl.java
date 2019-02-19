package com.ebsco.platform.shared.streamsets.stage.destination;

import com.ebsco.platform.shared.streamsets.Constants;
import com.ebsco.platform.shared.streamsets.Groups;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

import java.util.Map;

@StageDef(
        version = 1,
        label = "Kafka message splitter destination",
        description = "",
        icon = "default.png",
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.KafkaMFSGroup.class)
@GenerateResourceBundle
public class KafkaMessageSplitterDestinationImpl extends KafkaMessageSplitterDestination {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            defaultValue = "1024",
            label = "Chunk size (bytes)",
            displayPosition = 10,
            group = "MFS"
    )
    public int chunkSize;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "localhost:9092",
            label = "Brokers URI (ip:port[,ip:port])",
            displayPosition = 10,
            group = "Kafka"
    )
    public String brokers;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = Constants.KAFKA_RECORDS_TOPIC,
            label = "Topic name",
            displayPosition = 20,
            group = "Kafka"
    )
    public String topic;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            label = "Target consumer group id",
            description =
                    "Optional: if set the Producer will monitor the target topic consumption in Kafka.\n" +
                            "The producer will start processing the next incoming record (send rows to Kafka) " +
                            "only when all the messages has been consumed " +
                            "from the specified topic and group",
            displayPosition = 20,
            group = "Kafka"
    )
    public String targetConsumerGroupId;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            label = " Target consumer group access cache ttl, ms",
            description =
                    "Optional: Only works for non-empty 'Target consumer group id'.\n" +
                            "Since Kafka requests are too heavy and take too long time, " +
                            "the last topic/group lag request result is cached to reduce frequent interactions",
            defaultValue = "60000",
            displayPosition = 21,
            group = "Kafka"
            //dependsOn = "targetConsumerGroupId"
    )
    public Integer targetConsumerCacheTtlMs;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.NUMBER,
            label = "Target consumer group empty threshold, records.",
            description =
                    "Optional: Only works for non-empty 'Target consumer group id'.\n" +
                            "Producer will start processing the next incoming record " +
                            "only when the topic/group gets lower than the given value",
            defaultValue = "1000",
            displayPosition = 21,
            group = "Kafka"
            // dependsOn = "targetConsumerGroupId"
    )
    public Integer targetConsumerZeroLagThreshold;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MAP,
            defaultValue = "",
            label = "Producer custom parameters",
            displayPosition = 30,
            group = "Kafka"
    )
    public Map<String, String> kafkaProducerProperties;


    @Override
    public Integer getChunkSize() {
        return chunkSize;
    }

    @Override
    public String getBrokers() {
        return brokers;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getTargetConsumerGroupId() {
        return targetConsumerGroupId;
    }

    @Override
    public Integer getTargetConsumerCacheTtlMs() {
        return targetConsumerCacheTtlMs;
    }

    @Override
    public Integer getTargetConsumerZeroLagThreshold() {
        return targetConsumerZeroLagThreshold;
    }

    @Override
    public Map<String, String> getKafkaProducerProperties() {
        return kafkaProducerProperties;
    }

}
