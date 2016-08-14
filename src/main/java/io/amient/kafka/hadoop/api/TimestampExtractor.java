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

package io.amient.kafka.hadoop.api;

import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

public interface TimestampExtractor {

    /**
     *
     * Note: value.getBytes() returns a buffer which may be reused by the calling
     * code and its length may be bigger than the current value so getLength()
     * has to be used as the limit.
     *
     * @param key metadata associated with the message
     * @param value message payload byte buffer
     * @return timestamp utc in millisecond or null if no value could be extracted
     *          without exception.
     * @throws IOException
     */
    Long extract(MsgMetadataWritable key, BytesWritable value) throws IOException;
}

