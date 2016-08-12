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

import io.amient.kafka.hadoop.writable.MessageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class MessageRecordWriter extends RecordWriter<NullWritable, MessageWritable> {

    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    protected DataOutputStream out;

    static {
        try {
            newline = "\n".getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }

    public MessageRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    private void writeObject(MessageWritable msg) throws IOException {
        out.write(msg.getDataAsBytes());
        out.write(newline);
    }

    public synchronized void write(NullWritable key, MessageWritable value)
        throws IOException {

        boolean nullValue = value == null;
        if (nullValue) {
            return;
        }
        writeObject(value);
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
