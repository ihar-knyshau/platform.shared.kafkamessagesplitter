package com.ebsco.platform.shared.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;

public class KafkaMessageSplitter {
    public static final String UUID_KEY = "uuid";
    public static final String FINAL_CHUNK_KEY = "finalchunk";
    private int desiredChunkSize;

    public KafkaMessageSplitter(int desiredChunkSize) {
        this.desiredChunkSize = desiredChunkSize;
    }

    public List<byte[]> splitMessage(String strMessage) {
        List<byte[]> chunkList = new ArrayList<>();
        byte[] message = strMessage.getBytes(UTF_8);
        int messageSize = message.length;
        if (messageSize >= desiredChunkSize * 2) {
            int chunkNum = (int) Math.ceil(messageSize * 1.0 / desiredChunkSize);
            int chunkStart = 0;
            for (int i = 0; i < chunkNum; i++) {
                int chunkSize = chunkStart + desiredChunkSize <= messageSize ? desiredChunkSize : messageSize % desiredChunkSize;
                byte[] chunk = new byte[chunkSize];
                System.arraycopy(message, chunkStart, chunk, 0, chunkSize);
                chunkList.add(chunk);
                chunkStart = chunkStart + chunkSize;
            }
        } else {
            chunkList.add(message);
        }
        return chunkList;
    }

    public List<ProducerRecord<byte[], byte[]>> createChunkRecords(List<byte[]> chunkList, String topic) {
        List<ProducerRecord<byte[], byte[]>> recordList = new ArrayList<>();
        byte[] uuid = UUID.randomUUID().toString().getBytes(UTF_8);
        for (int i = 0; i < chunkList.size(); i++) {
            Iterable<Header> headers = null;
            if (i != chunkList.size() - 1) {
                headers = createChunkHeaders(uuid, false);
            } else {
                headers = createChunkHeaders(uuid, true);
            }
            byte[] chunk = chunkList.get(i);
            ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(topic, null, null, uuid, chunk, headers);
            recordList.add(record);
        }
        return recordList;
    }

    private Iterable<Header> createChunkHeaders(byte[] uuid, boolean finalChunk) {
        List<Header> headers = new LinkedList<>();
        RecordHeader uuidHeader = new RecordHeader(UUID_KEY, uuid);
        headers.add(uuidHeader);
        byte[] finalChunkBytes = new byte[1];
        finalChunkBytes[0] = finalChunk ? (byte) 1 : (byte) 0;
        RecordHeader finalChunkHeader = new RecordHeader(FINAL_CHUNK_KEY, finalChunkBytes);
        headers.add(finalChunkHeader);
        return headers;
    }
}
