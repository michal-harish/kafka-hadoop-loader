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
import io.amient.kafka.hadoop.writable.MessageMetadataWritableBuilder;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KafkaInputRecordReader extends RecordReader<MessageMetadataWritable, BytesWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaInputRecordReader.class);
    private static final long LATEST_TIME = -1L;
    private static final long EARLIEST_TIME = -2L;

    private Configuration conf;
    private KafkaInputSplit split;
    private SimpleConsumer consumer;
    private TopicAndPartition topicAndPartition;
    private String clientId;

    private long earliestOffset;
    private long latestOffset;
    private long watermark;

    private int fetchSize;
    private int timeout;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private MessageMetadataWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;
    private final MessageMetadataWritableBuilder keyBuilder = new MessageMetadataWritableBuilder();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        this.watermark = this.split.getWatermark();
        this.topicAndPartition = new TopicAndPartition(this.split.getTopic(), this.split.getPartition());
        this.fetchSize = conf.getInt("kafka.fetch.message.max.bytes", 1024 * 1024);
        this.clientId = conf.get("kafka.group.id");
        this.timeout = conf.getInt("kafka.socket.timeout.ms", 3000);
        int bufferSize = conf.getInt("kafka.socket.receive.buffer.bytes", 64 * 1024);

        consumer = new SimpleConsumer(this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize, clientId);

        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        String reset = conf.get("kafka.watermark.reset", "watermark");
        if ("earliest".equals(reset)) {
            resetWatermark(earliestOffset);
        } else if ("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (watermark < earliestOffset) {
            // We have lost some data, precisely between the watermark and the earliest offset
            LOG.warn(
                "Topic: {}, broker: {}, partition: {} ~ Resetting watermark as last read watermark was {} and the earliest available is {}",
                topicAndPartition.topic(),
                this.split.getBrokerId(),
                topicAndPartition.partition(),
                watermark,
                earliestOffset
            );

            resetWatermark(earliestOffset);
        }

        LOG.info(
            "Topic: {}, broker: {}, partition: {} ~ Split: {}, earliest: {}, latest: {}, starting: {}",
            topicAndPartition.topic(),
            this.split.getBrokerId(),
            topicAndPartition.partition(),
            this.split,
            earliestOffset,
            latestOffset,
            watermark
        );

        keyBuilder.setTopic(topicAndPartition.topic());
        keyBuilder.setBrokerHost(this.split.getBrokerHost());
        keyBuilder.setBrokerId(Integer.parseInt(this.split.getBrokerId()));
        keyBuilder.setPartition(topicAndPartition.partition());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (value == null) {
            value = new BytesWritable();
        }

        if (messages == null) {
            Map<TopicAndPartition, PartitionFetchInfo> requestInfo = new HashMap<>();

            PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(watermark, fetchSize);
            requestInfo.put(topicAndPartition, partitionFetchInfo);

            FetchRequest fetchRequest = new FetchRequestBuilder()
                .clientId(clientId)
                .addFetch(topicAndPartition.topic(), topicAndPartition.partition(), watermark, fetchSize)
                .build();

            LOG.info(
                "Topic: {}, broker: {}, partition: {} ~ fetching offset: {}",
                topicAndPartition.topic(),
                split.getBrokerId(),
                topicAndPartition.partition(),
                watermark
            );

            FetchResponse response = consumer.fetch(fetchRequest);
            messages = response.messageSet(topicAndPartition.topic(), topicAndPartition.partition());

            if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) == ErrorMapping
                .OffsetOutOfRangeCode()) {
                LOG.info("Out of bounds = " + watermark);
                return false;
            } else if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) != 0) {
                LOG.warn(
                    "Messages fetch error code: "
                    + response.errorCode(topicAndPartition.topic(), topicAndPartition.partition())
                );
                return false;
            } else {
                iterator = messages.iterator();
                if (!iterator.hasNext()) {
                    return false;
                }
            }
        }

        // Fill out the record fields using data from the next message.
        if (iterator.hasNext()) {
            MessageAndOffset messageOffset = iterator.next();

            watermark = messageOffset.nextOffset();
            Message message = messageOffset.message();

            key = keyBuilder.setOffset(messageOffset.offset()).createMessageSourceWritable();
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());

            numProcessedMessages++;
            if (!iterator.hasNext()) {
                messages = null;
                iterator = null;
            }
            return true;
        }
        LOG.warn("Unexpected iterator end");
        return false;
    }

    @Override
    public MessageMetadataWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float) (latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException {
        LOG.info(
            "Topic: {}, broker: {}, partition: {} ~ num. processed messages {}",
            topicAndPartition.topic(),
            split.getBrokerId(),
            topicAndPartition.partition(),
            numProcessedMessages
        );

        if (numProcessedMessages > 0) {
            KafkaZkUtils zk = new KafkaZkUtils(
                conf.get("kafka.zookeeper.connect"),
                conf.getInt("kafka.zookeeper.session.timeout.ms", 10000),
                conf.getInt("kafka.zookeeper.connection.timeout.ms", 10000)
            );

            String group = conf.get("kafka.group.id");
            zk.commitLastConsumedOffset(group, split.getTopic(), split.getPartition(), watermark);
            zk.close();
        }
        consumer.close();
    }

    private long getEarliestOffset() {
        // return kafka.api.OffsetRequest.EarliestTime();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(EARLIEST_TIME, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientId);

        if (earliestOffset <= 0) {
            OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
            earliestOffset = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        }
        return earliestOffset;
    }

    private long getLatestOffset() {
        // return kafka.api.OffsetRequest.LatestTime();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(LATEST_TIME, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientId);

        if (latestOffset <= 0) {
            OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
            latestOffset = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        }
        return latestOffset;
    }

    private void resetWatermark(long offset) {
        /**
         * Special edge case where the right offset is the value "0".
         *
         * Note: PartitionOffsetRequestInfo will return an empty array if given offset is "0".
         */
        if (offset == 0) {
            offset = EARLIEST_TIME;
        }

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(offset, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientId);

        LOG.info(
            "Topic: {}, broker: {}, partition: {} ~ Attempting to fetch offset where earliest was {}",
            topicAndPartition.topic(),
            this.split.getBrokerId(),
            topicAndPartition.partition(),
            offset
        );

        if (offset <= 0) {
            OffsetResponse offsetResponse = consumer.getOffsetsBefore(offsetRequest);
            offset = offsetResponse.offsets(topicAndPartition.topic(), topicAndPartition.partition())[0];
        }

        LOG.info(
            "Topic: {}, broker: {}, partition: {} ~ Resetting offset to: {}",
            topicAndPartition.topic() ,
            split.getBrokerId(),
            topicAndPartition.partition(),
            offset
        );

        watermark = earliestOffset = offset;
    }
}