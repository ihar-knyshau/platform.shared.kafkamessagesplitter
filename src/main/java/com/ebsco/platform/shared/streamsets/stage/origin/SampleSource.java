/**
 * Copyright 2015 StreamSets Inc.
 * <p>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ebsco.platform.shared.streamsets.stage.origin;

import com.ebsco.platform.shared.kafka.ChunksConsumer;
import com.ebsco.platform.shared.kafka.KafkaFactory;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * This source is an example and does not actually read from anywhere.
 * It does however, generate generate a simple record with one field.
 */
public abstract class SampleSource extends BasePushSource {

    private static final Logger LOG = LoggerFactory.getLogger(SampleSource.class);


    public abstract String getKafkaHost();

    public abstract String getKafkaPort();

    public abstract String getTopic();

    public abstract String getConsumerGroup();

    public abstract Long getCacheLifespan();

    private final CountDownLatch latch = new CountDownLatch(1);

    private Thread thread;

    @Override
    protected List<ConfigIssue> init() {
        initThread();
        return super.init();
    }

    private void initThread() {
        LOG.warn("STARTED INIT");
        thread = new Thread(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "quickstart.cloudera:9092");
            Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(props);

            consumer.subscribe(Collections.singletonList("test35"), new SampleSource.ChunkRebalanceListener((ChunksConsumer) consumer));

            ConsumerRecords<byte[], byte[]> consumerRecords;

            do {
                LOG.warn("POLL");
                consumerRecords = consumer.poll(Duration.ofMinutes(1));
                consumerRecords.records("test35").iterator().forEachRemaining(record -> {
                    LOG.warn("PRODUCE");
                    String composed = null;
                    String key = null;
                    try {
                        key = IOUtils.toString(record.key(), "UTF-8");
                        composed = IOUtils.toString(record.value(), "UTF-8");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    produceRecord(key, composed);
//        produceRecord("121346463", "wdwdwdwdwddwd");
                });
            } while (!this.thread.isInterrupted());
        });
        LOG.warn("FINISHED INIT");
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
        try {
            LOG.warn("STARTED");
            thread.start();
            latch.await();
            LOG.warn("FINISHED");
        } catch (InterruptedException e) {
            throw new RuntimeException("Thread was interrupted");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        LOG.warn("STARTED DESTROY");
        // Clean up any open resources.
        thread.interrupt();
        super.destroy();
        LOG.warn("FINISHED DESTROY");
    }


    @Override
    public int getNumberOfThreads() {
        return 1;
    }

    private void produceRecord(String id, String content) {
        Record record = createRecord(id, content);
        pushRecord(record);
    }

    private void pushRecord(Record record) {
        BatchContext batchContext = getContext().startBatch();
        batchContext.getBatchMaker().addRecord(record);
        getContext().processBatch(batchContext);
    }

    private Record createRecord(String message_id, String content) {
        Record record = getContext().createRecord(message_id);
        record.set(Field.create(content));
        return record;
    }

    class ChunkRebalanceListener implements ConsumerRebalanceListener {

        private ChunksConsumer chunksConsumer;

        public ChunkRebalanceListener(ChunksConsumer chunksConsumer) {
            this.chunksConsumer = chunksConsumer;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            chunksConsumer.setNewCachePartitions(partitions);
        }
    }
}
