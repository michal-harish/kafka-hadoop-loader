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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class CheckpointManager {
    private static final String CONFIG_KAFKA_GROUP_ID = "checkpoints.zookeeper.group.id";
    private static final String CONFIG_CHECKPOINTS_ZOOKEEPER = "checkpoint.use.zookeeper";
    private final FileSystem fs;
    private final Boolean useZkCheckpoints;
    private final KafkaZkUtils zkUtils;
    private final Configuration conf;
    private final Path hdfsCheckpointDir;

    public static void configureUseZooKeeper(Configuration conf, String kafkaGroupId) {
        conf.setBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, true);
        conf.set(CONFIG_KAFKA_GROUP_ID, kafkaGroupId);
    }

    public CheckpointManager(Configuration conf, KafkaZkUtils zkUtils) throws IOException {
        fs = FileSystem.get(conf);
        this.conf = conf;
        this.zkUtils = zkUtils;
        useZkCheckpoints = conf.getBoolean(CONFIG_CHECKPOINTS_ZOOKEEPER, false);
        hdfsCheckpointDir = new Path(conf.get("mapreduce.output.fileoutputformat.outputdir"), "_OFFSETS");
    }

    public long getNextOffsetToConsume(String topic, int partition) throws IOException {
        if (useZkCheckpoints) {
            String consumerGroup = conf.get(CONFIG_KAFKA_GROUP_ID);
            return zkUtils.getLastConsumedOffset(consumerGroup, topic, partition) + 1;
        } else {

            long nextOffsetToConsume = 0L;
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, topic + "-" + partition);
            if (fs.exists(comittedCheckpoint)) {
                try (FSDataInputStream in = fs.open(comittedCheckpoint)) {
                    in.readLong();// prev start offset
                    nextOffsetToConsume = in.readLong() + 1;
                }
            }
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, "_" + topic + "-" + partition);
            if (fs.exists(pendingCheckpoint)) {
                //TODO #11 locking either crashed job or another instance
                fs.delete(pendingCheckpoint, true);
            }
            try (FSDataOutputStream out = fs.create(pendingCheckpoint)) {
                out.writeLong(nextOffsetToConsume);
            }
            ///
            //
            return nextOffsetToConsume;
        }
    }

    public void commitOffsets(String topic, int partition, long lastConsumedOffset) throws IOException {
        if (useZkCheckpoints) {
            String group = conf.get(CONFIG_KAFKA_GROUP_ID);
            zkUtils.commitLastConsumedOffset(group, topic, partition, lastConsumedOffset);
        } else {
            Path pendingCheckpoint = new Path(hdfsCheckpointDir, "_" + topic + "-" + partition);
            Path comittedCheckpoint = new Path(hdfsCheckpointDir, topic + "-" + partition);

            try(FSDataOutputStream out = fs.append(pendingCheckpoint)) {
                out.writeLong(lastConsumedOffset);
            }
            fs.rename(pendingCheckpoint, comittedCheckpoint);
        }
    }
}
