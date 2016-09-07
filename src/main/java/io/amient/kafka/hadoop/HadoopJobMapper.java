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

import io.amient.kafka.hadoop.api.Extractor;
import io.amient.kafka.hadoop.api.TimestampExtractor;
import io.amient.kafka.hadoop.api.Transformation;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HadoopJobMapper extends Mapper<MsgMetadataWritable, BytesWritable, MsgMetadataWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(HadoopJobMapper.class);

    private static final String CONFIG_EXTRACTOR_CLASS = "mapper.timestamp.extractor.class";

    private Extractor extractor;

    /**
     * Provide a timestamp extractor
     * @param conf
     * @param cls class implementing the TimestampExtractor interface
     */
    public static void configureExtractor(Configuration conf, Class<? extends Extractor> cls) {
        conf.set(CONFIG_EXTRACTOR_CLASS, cls.getName());
    }

    public static boolean isTimestampExtractorConfigured(Configuration conf) throws IOException {
        String extractorClassName = conf.get(CONFIG_EXTRACTOR_CLASS, null);
        if (extractorClassName == null) return false; else {
            try {
                return TimestampExtractor.class.isAssignableFrom(Class.forName(extractorClassName));
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        try {
            Class<?> extractorClass = conf.getClass(CONFIG_EXTRACTOR_CLASS, null);
            if (extractorClass != null) {
                extractor = extractorClass.asSubclass(Extractor.class).newInstance();
                log.info("Using extractor " + extractor);
            }

        } catch (Exception e) {
            throw new IOException(e);
        }
        super.setup(context);
    }

    @Override
    public void map(MsgMetadataWritable key, BytesWritable value, Context context) throws IOException {
        try {
            if (key != null) {
                MsgMetadataWritable outputKey = key;
                BytesWritable outputValue = value;
                if (extractor != null) {
                    Object any = extractor.deserialize(key, value);
                    if (extractor instanceof TimestampExtractor) {
                        Long timestamp = ((TimestampExtractor)extractor).extractTimestamp(any);
                        outputKey = new MsgMetadataWritable(key, timestamp);
                    }
                    if (extractor instanceof Transformation) {
                        outputValue = ((Transformation)extractor).transform(any);
                    }
                }
                context.write(outputKey, outputValue);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            throw e;
        }
    }

}