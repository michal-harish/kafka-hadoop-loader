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

package io.amient.kafka.hadoop.writable;

public class MessageMetadataWritableBuilder {

    private String brokerHost;
    private String topic;
    private Integer brokerId;
    private Integer partition;
    private Long offset;

    public MessageMetadataWritableBuilder() {
        resetBuilder();
    }

    public MessageMetadataWritableBuilder setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public MessageMetadataWritableBuilder setBrokerHost(String brokerHost) {
        this.brokerHost = brokerHost;
        return this;
    }

    public MessageMetadataWritableBuilder setBrokerId(int brokerId) {
        this.brokerId = Integer.valueOf(brokerId);
        return this;
    }

    public MessageMetadataWritableBuilder setPartition(int partition) {
        this.partition = Integer.valueOf(partition);
        return this;
    }

    public MessageMetadataWritableBuilder setOffset(long offset) {
        this.offset = Long.valueOf(offset);
        return this;
    }

    public void resetBuilder() {
        brokerHost = "";
        topic = null;
        brokerId = null;
        partition = null;
        offset = null;
    }

    public MessageMetadataWritable createMessageSourceWritable() {
        return new MessageMetadataWritable(topic, brokerId, brokerHost, partition, offset);
    }
}