package com.ebsco.platform.shared.streamsets;

import com.streamsets.pipeline.api.Label;

import java.util.HashMap;
import java.util.Map;

public class Groups {
    public static final String CHUNK_CACHE = "Chunk cache";
    public static final String KAFKA = "Kafka";


    interface CustomLabel<T extends Enum> extends Label {
        Map<String, String> labels = new HashMap<>();

        default void addLabel(String name, String value) {
            labels.put(name, value);
        }

        @Override
        default String getLabel() {
            return labels.get(((T) this).name());
        }
    }


    /**
     * Kafka group for MFS
     */
    public enum KafkaChunkedGroup implements CustomLabel<KafkaChunkedGroup> {
        ChunkCache(Groups.CHUNK_CACHE),
        Kafka(Groups.KAFKA);

        KafkaChunkedGroup(String label) {
            addLabel(this.name(), label);
        }
    }


}