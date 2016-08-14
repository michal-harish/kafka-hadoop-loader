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

package io.amient.kafka.hadoop.io;

import io.amient.kafka.hadoop.CheckpointManager;
import io.amient.kafka.hadoop.KafkaZkUtils;
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

public class KafkaInputRecordReader extends RecordReader<MsgMetadataWritable, BytesWritable> {

    private static final Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);
    private static final long LATEST_TIME = -1L;
    private static final long EARLIEST_TIME = -2L;
    final private static String CLIENT_ID = "kafka-hadoop-loader";

    private Configuration conf;
    private KafkaInputSplit split;
    private SimpleConsumer consumer;
    private TopicAndPartition topicAndPartition;

    private long earliestOffset;
    private long latestOffset;

    private long nextOffsetToConsume;

    private int fetchSize;
    private int timeout;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private MsgMetadataWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        this.nextOffsetToConsume = this.split.getStartOffset();
        this.topicAndPartition = new TopicAndPartition(this.split.getTopic(), this.split.getPartition());
        this.fetchSize = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_MESSAGE_MAX_BYTES, 1024 * 1024);
        this.timeout = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_SOCKET_TIMEOUT_MS, 3000);
        int bufferSize = conf.getInt(KafkaInputFormat.CONFIG_KAFKA_RECEIVE_BUFFER_BYTES, 64 * 1024);

        consumer = new SimpleConsumer(
                this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize, CLIENT_ID);

        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        String reset = conf.get(KafkaInputFormat.CONFIG_KAFKA_AUTOOFFSET_RESET, "watermark");
        if ("earliest".equals(reset)) {
            resetWatermark(earliestOffset);
        } else if ("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (nextOffsetToConsume < earliestOffset) {
            // We have lost some data, precisely between the watermark and the earliest offset
            log.warn(
                "Topic: {}, broker: {}, partition: {} ~ Resetting watermark as last watermark was {} and the earliest available is {}",
                topicAndPartition.topic(),
                this.split.getBrokerId(),
                topicAndPartition.partition(),
                nextOffsetToConsume,
                earliestOffset
            );

            resetWatermark(earliestOffset);
        }

        log.info(
            "Topic: {}, broker: {}, partition: {} ~ Split: {}, earliest: {}, latest: {}, starting: {}",
            topicAndPartition.topic(),
            this.split.getBrokerId(),
            topicAndPartition.partition(),
            this.split,
            earliestOffset,
            latestOffset,
            nextOffsetToConsume
        );

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (value == null) {
            value = new BytesWritable();
        }

        if (messages == null) {
            Map<TopicAndPartition, PartitionFetchInfo> requestInfo = new HashMap<>();

            PartitionFetchInfo partitionFetchInfo = new PartitionFetchInfo(nextOffsetToConsume, fetchSize);
            requestInfo.put(topicAndPartition, partitionFetchInfo);

            FetchRequest fetchRequest = new FetchRequestBuilder()
                .clientId(CLIENT_ID)
                .addFetch(topicAndPartition.topic(), topicAndPartition.partition(), nextOffsetToConsume, fetchSize)
                .build();

            log.info(
                "Topic: {}, broker: {}, partition: {} ~ fetching offset: {}",
                topicAndPartition.topic(),
                split.getBrokerId(),
                topicAndPartition.partition(),
                nextOffsetToConsume
            );

            FetchResponse response = consumer.fetch(fetchRequest);
            messages = response.messageSet(topicAndPartition.topic(), topicAndPartition.partition());

            if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) == ErrorMapping
                .OffsetOutOfRangeCode()) {
                log.info("Out of bounds = " + nextOffsetToConsume);
                return false;
            } else if (response.errorCode(topicAndPartition.topic(), topicAndPartition.partition()) != 0) {
                log.warn(
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

        if (iterator.hasNext()) {
            MessageAndOffset messageOffset = iterator.next();

            nextOffsetToConsume = messageOffset.nextOffset();
            Message message = messageOffset.message();

            key = new MsgMetadataWritable(split, messageOffset.offset());

            if (message.isNull()) {
                value.setSize(0);
            } else {
                value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            }

            numProcessedMessages++;
            if (!iterator.hasNext()) {
                messages = null;
                iterator = null;
            }
            return true;
        }
        log.warn("Unexpected iterator end");
        return false;
    }

    @Override
    public MsgMetadataWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (nextOffsetToConsume > latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (nextOffsetToConsume - earliestOffset) / (float) (latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException {
        log.info(
            "Topic: {}, broker: {}, partition: {} ~ num. processed messages {}",
            topicAndPartition.topic(),
            split.getBrokerId(),
            topicAndPartition.partition(),
            numProcessedMessages
        );

        if (numProcessedMessages > 0) {
            try(KafkaZkUtils zk = new KafkaZkUtils(
                conf.get(KafkaInputFormat.CONFIG_ZK_CONNECT),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000),
                conf.getInt(KafkaInputFormat.CONFIG_ZK_SESSION_TIMEOUT_MS, 10000)
            )) {
                new CheckpointManager(conf, zk)
                        .commitOffsets(split.getTopic(), split.getPartition(), nextOffsetToConsume - 1);
            }
        }
        consumer.close();
    }

    private long getEarliestOffset() {
        // return kafka.api.OffsetRequest.EarliestTime();
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(EARLIEST_TIME, 1));
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), CLIENT_ID);

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
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), CLIENT_ID);

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
        OffsetRequest offsetRequest = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), CLIENT_ID);

        log.info(
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

        log.info(
            "Topic: {}, broker: {}, partition: {} ~ Resetting offset to: {}",
            topicAndPartition.topic() ,
            split.getBrokerId(),
            topicAndPartition.partition(),
            offset
        );

        nextOffsetToConsume = earliestOffset = offset;
    }
}