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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MessageMetadataWritable implements Writable {

    private String topic;
    private String brokerHost;
    private int brokerId;
    private int partition;
    private long offset;

    /**
     * @deprecated Constructor used by Hadoop to init the class through reflection. Do not remove...
     */
    public MessageMetadataWritable() {
    }

    public MessageMetadataWritable(String topic, int brokerId, String brokerHost, int partition, long offset) {
        this.topic = topic;
        this.brokerId = brokerId;
        this.partition = partition;
        this.offset = offset;
        this.brokerHost = brokerHost;
    }

    public long getOffset() {
        return offset;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public int getPartition() {
        return partition;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataOutput out) throws IOException {
        // Write out partition info using variable length encodings
        WritableUtils.writeVInt(out, getBrokerId());
        WritableUtils.writeVInt(out, getPartition());
        WritableUtils.writeVLong(out, getOffset());

        // writeString writes the string length out as a VInt then the bytes themselves
        Text.writeString(out, getTopic());
        Text.writeString(out, getBrokerHost());
    }

    /** {@inheritDoc} */
    @Override
    public void readFields(DataInput in) throws IOException {
        // Read partition info
        brokerId = WritableUtils.readVInt(in);
        partition = WritableUtils.readVInt(in);
        offset = WritableUtils.readVLong(in);

        // readString reads the string length out as a VInt for buffer size then the bytes themselves
        topic = Text.readString(in);
        brokerHost = Text.readString(in);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return String.format("Topic: %s BrokerId: %s BrokerHost: %s Partition: %s Offset: %s",
            getTopic(),
            String.valueOf(getBrokerId()),
            getBrokerHost(),
            String.valueOf(getPartition()),
            String.valueOf(getOffset()));
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MessageMetadataWritable)) {
            return false;
        }

        MessageMetadataWritable that = (MessageMetadataWritable) o;

        if (brokerId != that.brokerId) {
            return false;
        } else if (offset != that.offset) {
            return false;
        } else if (partition != that.partition) {
            return false;
        } else if (!topic.equals(that.topic)) {
            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + brokerId;
        result = 31 * result + partition;
        result = 31 * result + (int) (offset ^ (offset >>> 32));
        return result;
    }

}
