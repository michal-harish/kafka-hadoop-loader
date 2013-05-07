/*
 * Copyright 2012 Michal Harish, michal.harish@gmail.com
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

package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

        List<InputSplit> splits = new ArrayList<InputSplit>();

        Configuration conf = context.getConfiguration();
        List<String> topics = Arrays.asList(conf.get("kafka.topics").split(","));
        ZkUtils zk = new ZkUtils(
            conf.get("kafka.zk.connect"),
            conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
            conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
        );
        List<String> seeds = zk.getSeedList();

        String consumerGroup = conf.get("kafka.groupid");

        for(final String seed: seeds) {
            SimpleConsumer consumer = new SimpleConsumer(seed, 9092, 10000, 65535, "PartitionsLookup");
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            if (response != null && response.topicsMetadata() != null) {
                for(TopicMetadata tm: response.topicsMetadata()) {
                    for(PartitionMetadata pm: tm.partitionsMetadata()) {
                        long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, tm.topic(), pm.partitionId()) ;
                        InputSplit split = new KafkaInputSplit(
                            seed, 
                            tm.topic(), 
                            pm.partitionId(), 
                            lastConsumedOffset
                        );
                        splits.add(split);
                    }
                }
            }
        }
        zk.close();
        return splits;
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new KafkaInputRecordReader() ;
    }

}
