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

package io.amient.kafka.hadoop;

import io.amient.kafka.hadoop.writable.MessageMetadataWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class HadoopJobMapper extends Mapper<MessageMetadataWritable, BytesWritable, MessageMetadataWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(HadoopJobMapper.class);

    //private Deserializer kafkaDeserializer;
    //private hadoopFormatSerializer


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //TODO initialise deserialiser from configuration by class name
        //TODO intialise partitioner from configuration
        //TODO initialise timestamp extractor if partitioning by date is enabled
    }

    @Override
    public void map(MessageMetadataWritable key, BytesWritable value, Context context) throws IOException {
        try {
            context.write(key, map(key, value));
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public BytesWritable map(MessageMetadataWritable key, BytesWritable value) {
        BytesWritable outputValue = null;
        if (key != null) {
            if (value.getBytes().length > 0) {
                outputValue = new BytesWritable();
                //TODO invoke plugabble output formatter - i.e. schemaless, test concrete json
                Long timestamp = 0L;
                //TODO partition by topic
                //TODO partition by time -> formatted - prepare for kafka message containing timestamp
                //TODO partition by key
                SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd H");
                f.setTimeZone(TimeZone.getTimeZone("UTC"));
                String[] dateTime = f.format(new java.util.Date(timestamp)).split(" ");

            }
        }
        return outputValue;
    }
}