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
import java.util.List;

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

    private KafkaInputFetcher fetcher;
    private int fetchSize;
    private String topic;
    private String reset;

    private int partition;
    private long smallestOffset;
    private long watermark;
    private long latestOffset;
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
        this.topic = this.split.getTopic();
        this.partition = this.split.getPartition();
        this.watermark = this.split.getWatermark();

        ZkUtils zk = new ZkUtils(
            conf.get("kafka.zk.connect"),
            conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
            conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
        );
        List<String> seeds = zk.getSeedList();
        zk.close();

        int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
        int bufferSize = conf.getInt("kafka.socket.buffersize", 64*1024);
        fetcher = new KafkaInputFetcher(clientId, topic, partition, seeds, timeout, bufferSize);
        fetchSize = conf.getInt("kafka.fetch.size", 1024 * 1024);
        reset = conf.get("kafka.watermark.reset", "watermark");
        smallestOffset = fetcher.getOffset(kafka.api.OffsetRequest.EarliestTime());
        latestOffset = fetcher.getOffset(kafka.api.OffsetRequest.LatestTime());

        if ("smallest".equals(reset)) {
            resetWatermark(kafka.api.OffsetRequest.EarliestTime());
        } else if("largest".equals(reset)) {
            resetWatermark(kafka.api.OffsetRequest.LatestTime());
        } else if (watermark < smallestOffset) {
            resetWatermark(kafka.api.OffsetRequest.EarliestTime());
        }

        log.info(
            "Split {} Topic: {} Partition: {} Smallest: {} Largest: {} Starting: {}", 
            new Object[]{this.split, topic, partition, smallestOffset, latestOffset, watermark }
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
        MessageAndOffset messageAndOffset = fetcher.nextMessageAndOffset(fetchSize);
        if (messageAndOffset == null) {
            return false;
        } else {
            key.set(messageAndOffset.offset());
            Message message = messageAndOffset.message();
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            numProcessedMessages++;
            watermark = messageAndOffset.nextOffset();
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
        if (watermark >= latestOffset || smallestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - smallestOffset) / (float)(latestOffset - smallestOffset));
    }

    @Override
    public void close() throws IOException
    {
        log.info("{} num. processed messages {} ", topic+":" + partition, numProcessedMessages);
        if (numProcessedMessages >0)
        {
            ZkUtils zk = new ZkUtils(
                conf.get("kafka.zk.connect"),
                conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
                conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
            );

            String group = conf.get("kafka.groupid");
            zk.commitLastConsumedOffset(group, split.getTopic(), split.getPartition(), watermark);
            zk.close();
        }
        fetcher.close();
    }

    private void resetWatermark(Long offset) {
        if (offset.equals(kafka.api.OffsetRequest.LatestTime())
            || offset.equals(kafka.api.OffsetRequest.EarliestTime())
        ) {
            offset = fetcher.getOffset(offset);
        }
        log.info("{} resetting offset to {}", topic+":" + partition, offset);
        watermark = smallestOffset = offset;
    }


}
