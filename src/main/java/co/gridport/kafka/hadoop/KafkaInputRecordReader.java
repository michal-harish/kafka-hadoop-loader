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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputRecordReader extends RecordReader<LongWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private Configuration conf;

    private KafkaInputSplit split;

    private SimpleConsumer consumer ;
    private int fetchSize;
    private String topic;
    private String reset;

    private int partition;
    private long earliestOffset;
    private long watermark;
    private long latestOffset;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private LongWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;
    
    private String clientId = "hadoop-loader";

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException
    {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        topic = this.split.getTopic();
        partition = this.split.getPartition();
        watermark = this.split.getWatermark();

        int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
        int bufferSize = conf.getInt("kafka.socket.buffersize", 64*1024);
        consumer =  new SimpleConsumer(
            this.split.getBrokerHost(), this.split.getBrokerPort(),
            timeout, 
            bufferSize, 
            clientId
        );

        fetchSize = conf.getInt("kafka.fetch.size", 1024 * 1024);
        reset = conf.get("kafka.watermark.reset", "watermark");
        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        //log.info("Last watermark for {} to {}", topic +":"+partition, watermark);

        if ("earliest".equals(reset)) {
            resetWatermark(-1);
        } else if("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (watermark < earliestOffset) {
            resetWatermark(-1);
        }

        log.info(
            "Split {} Topic: {} Broker: {} Partition: {} Earliest: {} Latest: {} Starting: {}", 
            new Object[]{this.split, topic, this.split.getBrokerId(), partition, earliestOffset, latestOffset, watermark }
        );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }

        if (messages == null) {
            FetchRequest req = new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(topic, partition, watermark, fetchSize)
                .build();
            log.info("{} fetching offset {} ", topic+":" + split.getBrokerId() +":" + partition, watermark);
            FetchResponse response = consumer.fetch(req);
            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    watermark = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), clientId);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            } else {
                iterator = messages.iterator();
                watermark += messages.validBytes();
                if (!iterator.hasNext())
                {
                    //log.info("No more messages");
                    return false;
                }
            }
        }

        if (iterator.hasNext())
        {
            MessageAndOffset messageOffset = iterator.next();
            Message message = messageOffset.message();
            key.set(watermark - message.size() - 4);
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            numProcessedMessages++;
            if (!iterator.hasNext())
            {
                messages = null;
                iterator = null;
            }
            return true;
        }
        log.warn("Unexpected iterator end.");
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException 
    {
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float)(latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException
    {
        log.info("{} num. processed messages {} ", topic+":" + split.getBrokerId() +":" + partition, numProcessedMessages);
        if (numProcessedMessages >0)
        {
            ZkUtils zk = new ZkUtils(
                conf.get("kafka.zk.connect"),
                conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
                conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
            );

            String group = conf.get("kafka.groupid");
            String partition = split.getBrokerId() + "-" + split.getPartition();
            zk.commitLastConsumedOffset(group, split.getTopic(), partition, watermark);
            zk.close();
        }
        consumer.close();
    }

    private long getEarliestOffset() {
        if (earliestOffset <= 0) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
            OffsetResponse response = consumer.getOffsetsBefore(request);

            if (response.hasError()) {
                System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
                return 0;
            }
            long[] offsets = response.offsets(topic, partition);
            earliestOffset = offsets[0];
        }
        return earliestOffset;
    }

    private long getLastOffset() {
        if (latestOffset <= 0) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
            OffsetResponse response = consumer.getOffsetsBefore(request);

            if (response.hasError()) {
                System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
                return 0;
            }
            long[] offsets = response.offsets(topic, partition);
            latestOffset = offsets[0];
        }
        return latestOffset;
    }

    private void resetWatermark(long offset) {
        if (offset <= 0) {
            offset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
        }
        log.info("{} resetting offset to {}", topic+":" + split.getBrokerId() +":" + partition, offset);
        watermark = earliestOffset = offset;
    }

    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

}
