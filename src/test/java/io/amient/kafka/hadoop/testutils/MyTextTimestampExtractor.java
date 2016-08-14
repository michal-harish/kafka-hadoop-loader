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

package io.amient.kafka.hadoop.testutils;

import io.amient.kafka.hadoop.api.TimestampExtractor;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.io.BytesWritable;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

public class MyTextTimestampExtractor implements TimestampExtractor {
    SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public long extract(MsgMetadataWritable key, BytesWritable value) throws RuntimeException {
        try {
            String leadString = new String(Arrays.copyOfRange(value.getBytes(), 0, 19));
            return parser.parse(leadString).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public String format(long timestamp) {
        return parser.format(timestamp);
    }
}
