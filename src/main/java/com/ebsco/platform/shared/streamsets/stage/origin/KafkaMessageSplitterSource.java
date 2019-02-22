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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class KafkaMessageSplitterSource extends BasePushSource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageSplitterSource.class);
    private static final int THREAD_COUNT = 1;

    public abstract String getKafkaHost();

    public abstract String getKafkaPort();

    public abstract String getTopic();

    public abstract String getConsumerGroup();

    public abstract Long getCacheLifespan();

    private ExecutorService executorService = Executors.newFixedThreadPool(getNumberOfThreads());

    @Override
    protected List<ConfigIssue> init() {
        return super.init();
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
        List<Future<Runnable>> futures = new ArrayList<>(getNumberOfThreads());
        for(int i = 0; i < getNumberOfThreads(); i++) {
            Future future = executorService.submit(new ChunksConsumerRunnable(this));
            futures.add(future);
        }
        for(Future<Runnable> f : futures) {
            try {
                f.get();
            } catch (InterruptedException| ExecutionException e) {
                LOG.error("Record generation threads have been interrupted", e.getMessage());
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        LOG.warn("DESTROY STARTED");
        executorService.shutdownNow();
        LOG.warn("DESTROY FIN");
        super.destroy();
    }


    @Override
    public int getNumberOfThreads() {
        return THREAD_COUNT;
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
        Map<String, Field> map = new LinkedHashMap<>();
        map.put("text", Field.create(content));
        record.set(Field.create(map));
        return record;
    }

    static class ChunkRebalanceListener implements ConsumerRebalanceListener {

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

    static class ChunksConsumerRunnable implements Runnable {

        private KafkaMessageSplitterSource source;

        public ChunksConsumerRunnable(KafkaMessageSplitterSource source) {
            this.source = source;
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, source.getKafkaHost() + ":" + source.getKafkaPort());
            props.put(ConsumerConfig.GROUP_ID_CONFIG, source.getConsumerGroup());
            try {
                Consumer<byte[], byte[]> consumer = KafkaFactory.createConsumer(props);

                consumer.subscribe(Collections.singletonList(source.getTopic()), new KafkaMessageSplitterSource.ChunkRebalanceListener((ChunksConsumer) consumer));

                ConsumerRecords<byte[], byte[]> consumerRecords;

                do {
                    consumerRecords = consumer.poll(Duration.ofMinutes(1));
                    consumerRecords.records(source.getTopic()).iterator().forEachRemaining(record -> {
                        String composed = null;
                        String key = null;
                        try {
                            key = IOUtils.toString(record.key(), "UTF-8");
                            composed = IOUtils.toString(record.value(), "UTF-8");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        source.produceRecord(key, composed);
                    });
                } while (!source.getContext().isStopped());
            } catch (Exception e) {
                LOG.error("CONSUMER ERROR");
                LOG.error(e.getCause().getMessage());
                StackTraceElement[] s = e.getCause().getStackTrace();
                for (StackTraceElement se : s) {
                    LOG.error("\tat " + se);
                }
            }
        }
    }
}
