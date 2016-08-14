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
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

public class MyJsonTimestampExtractor implements TimestampExtractor {

    ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public Long extract(MsgMetadataWritable key, BytesWritable value) throws IOException {
        if (value.getLength() > 0) {
            JsonNode json = jsonMapper.readValue(value.getBytes(), 0, value.getLength(), JsonNode.class);
            if (json.has("timestamp")) {
                return json.get("timestamp").getLongValue();
            }
        }
        return null;
    }

}