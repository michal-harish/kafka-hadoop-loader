/*
 * Copyright 2014 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.kafka.hadoop;

import io.amient.kafka.hadoop.format.KafkaInputSplit;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaInputSplitsBuilder {

    public static List<KafkaInputSplit> createInputSplitPerPartitionLeader(
        KafkaZkUtils kafkaMetadataClient,
        Configuration configuration
        ) throws IOException {
        String[] inputTopics = configuration.get("kafka.topics").split(",");
        String consumerGroup = configuration.get("kafka.group.id");

        List<KafkaInputSplit> splits = new ArrayList<>();
        for (String topic : inputTopics) {
            Map<Integer, Integer> partitionLeaders = kafkaMetadataClient.getPartitionLeaders(topic);
            for (int partition : partitionLeaders.keySet()) {
                int brokerId = partitionLeaders.get(partition);
                long lastConsumedOffset = kafkaMetadataClient.getLastConsumedOffset(consumerGroup, topic, partition);

                KafkaInputSplit split = new KafkaInputSplit(
                    brokerId,
                    kafkaMetadataClient.getBrokerName(brokerId),
                    topic,
                    partition,
                    lastConsumedOffset
                    );

                splits.add(split);
            }
        }

        return splits;
    }
}
