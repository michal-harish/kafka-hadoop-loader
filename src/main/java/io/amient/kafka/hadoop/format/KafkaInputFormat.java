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

package io.amient.kafka.hadoop.format;

import io.amient.kafka.hadoop.KafkaZkUtils;
import io.amient.kafka.hadoop.writable.MessageMetadataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KafkaInputFormat extends InputFormat<MessageMetadataWritable, BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try (KafkaZkUtils zk = new KafkaZkUtils(
                conf.get("kafka.zookeeper.connect"),
                conf.getInt("kafka.zookeeper.session.timeout.ms", 10000),
                conf.getInt("kafka.zookeeper.connection.timeout.ms", 10000)
        )) {
            return createInputSplitPerPartitionLeader(zk, conf);
        }

    }

    @Override
    public RecordReader<MessageMetadataWritable, BytesWritable> createRecordReader(
        InputSplit inputSplit,
        TaskAttemptContext context
        ) throws IOException, InterruptedException {
        return new KafkaInputRecordReader();
    }

    private List<InputSplit> createInputSplitPerPartitionLeader(
            KafkaZkUtils zkUtils,
            Configuration configuration
    ) throws IOException {
        String[] inputTopics = configuration.get("kafka.topics").split(",");
        String consumerGroup = configuration.get("kafka.group.id");

        List<InputSplit> splits = new ArrayList<>();
        for (String topic : inputTopics) {
            Map<Integer, Integer> partitionLeaders = zkUtils.getPartitionLeaders(topic);
            for (int partition : partitionLeaders.keySet()) {
                int brokerId = partitionLeaders.get(partition);
                long lastConsumedOffset = zkUtils.getLastConsumedOffset(consumerGroup, topic, partition);

                KafkaInputSplit split = new KafkaInputSplit(
                        brokerId,
                        zkUtils.getBrokerName(brokerId),
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