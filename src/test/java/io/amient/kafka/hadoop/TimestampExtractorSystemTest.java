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

import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MultiOutputFormat;
import io.amient.kafka.hadoop.testutils.MyJsonTimestampExtractor;
import io.amient.kafka.hadoop.testutils.SystemTestBase;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimestampExtractorSystemTest extends SystemTestBase {


    @Test
    public void canUseTimestampInPartitions() throws IOException, ClassNotFoundException, InterruptedException {

        //produce some json data
        String message5 = "{\"version\":5,\"timestamp\":1402944501425,\"id\": 1}";
        simpleProducer.send(new KeyedMessage<>("topic02", "1", message5));
        String message1 = "{\"version\":1,\"timestamp\":1402945801425,\"id\": 2}";
        simpleProducer.send(new KeyedMessage<>("topic02", "2", message1));
        String message6 = "{\"version\":6,\"timestamp\":1402948801425,\"id\": 1}";
        simpleProducer.send(new KeyedMessage<>("topic02", "1", message6));
        //testing a null message - with timestamp extractor this means skip message
        simpleProducer.send(new KeyedMessage<>("topic02", "1", (String)null));

        //configure inputs, timestamp extractor and the output path format
        KafkaInputFormat.configureKafkaTopics(conf, "topic02");
        KafkaInputFormat.configureZkConnection(conf, zkConnect);
        HadoopJobMapper.configureExtractor(conf, MyJsonTimestampExtractor.class);
        MultiOutputFormat.configurePathFormat(conf, "'t={T}/d='yyyy-MM-dd'/h='HH");

        Path outDir = runSimpleJob("topic02", "canUseTimestampInPartitions");

        Path h18 = new Path(outDir, "t=topic02/d=2014-06-16/h=18/topic02-1-0000000000000000000");
        assertTrue(localFileSystem.exists(h18));
        assertEquals(String.format("%s%n", message5), readFullyAsString(h18, 100));

        Path h19 = new Path(outDir, "t=topic02/d=2014-06-16/h=19/topic02-0-0000000000000000000");
        assertTrue(localFileSystem.exists(h19));
        assertEquals(String.format("%s%n", message1), readFullyAsString(h19, 100));

        Path h20 = new Path(outDir, "t=topic02/d=2014-06-16/h=20/topic02-1-0000000000000000000");
        assertTrue(localFileSystem.exists(h20));
        assertEquals(String.format("%s%n", message6), readFullyAsString(h20, 100));
    }


}
