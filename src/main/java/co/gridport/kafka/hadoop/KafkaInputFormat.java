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
import java.util.List;

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

        //TODO utilise TopicMetadataRequest of simple consumer
        Configuration conf = context.getConfiguration();
        ZkUtils zk = new ZkUtils(
            conf.get("kafka.zk.connect"),
            conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
            conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
        );
        String[] inputTopics = conf.get("kafka.topics").split(",");
        String consumerGroup = conf.get("kafka.groupid");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for(String topic: inputTopics)
        {
            List<String> brokerPartitions = zk.getBrokerPartitions(topic);
            for(String partition: brokerPartitions) {
                String[] brokerPartitionParts = partition.split("-");
                String brokerId = brokerPartitionParts[0];
                String partitionId = brokerPartitionParts[1];
                long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, topic, partition) ;
                InputSplit split = new KafkaInputSplit(
                    brokerId, 
                    zk.getBrokerName(brokerId), 
                    topic, 
                    Integer.valueOf(partitionId), 
                    lastConsumedOffset
                );
                splits.add(split);
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
