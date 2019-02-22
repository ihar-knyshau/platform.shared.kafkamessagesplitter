/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ebsco.platform.shared.streamsets.stage.origin;

import com.ebsco.platform.shared.streamsets.Groups;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;

@StageDef(
    version = 1,
    label = "Kafka Split Message",
    description = "",
//    icon = "default.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.KafkaChunkedGroup.class)
@GenerateResourceBundle
public class KafkaMessageSplitterDSource extends KafkaMessageSplitterSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "localhost",
      label = "Kafka Host",
      displayPosition = 10,
      group = "Kafka"
  )
  public String kafkaHost;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "9092",
          label = "Kafka Port",
          displayPosition = 10,
          group = "Kafka"
  )
  public String kafkaPort;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "test",
          label = "Kafka Topic",
          displayPosition = 10,
          group = "Kafka"
  )
  public String kafkaTopic;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "test",
          label = "Kafka Consumer Group",
          displayPosition = 10,
          group = "Kafka"
  )
  public String kafkaConsumerGroup;

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.NUMBER,
          defaultValue = "180000",
          label = "Consumer cache lifespan in millis",
          displayPosition = 10,
          group = "ChunkCache"
  )
  public Long kafkaCacheLifespan;


  @Override
  public String getKafkaHost() {
    return kafkaHost;
  }

  @Override
  public String getKafkaPort() {
    return kafkaPort;
  }

  @Override
  public String getTopic() {
    return kafkaTopic;
  }

  @Override
  public String getConsumerGroup() {
    return kafkaConsumerGroup;
  }

  @Override
  public Long getCacheLifespan() {
    return kafkaCacheLifespan;
  }
}
