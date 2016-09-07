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

import io.amient.kafka.hadoop.testutils.SystemTestBase;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultSystemTest extends SystemTestBase {
    @Test
    public void canFollowKafkaPartitionsIncrementally()
            throws IOException, ClassNotFoundException, InterruptedException {

        //produce text data
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadA"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key2", "payloadB"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadC"));

        //run the first job
        runSimpleJob("topic01", "canFollowKafkaPartitions");

        //produce more data
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadD"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key2", "payloadE"));

        //run the second job
        Path result = runSimpleJob("topic01", "canFollowKafkaPartitions");

        //check results
        Path part0offset0 = new Path(result, "topic01/0/topic01-0-0000000000000000000");
        assertTrue(localFileSystem.exists(part0offset0));
        assertEquals(String.format("payloadA%npayloadC%n"), readFullyAsString(part0offset0, 20));

        Path part0offset2 = new Path(result, "topic01/0/topic01-0-0000000000000000002");
        assertTrue(localFileSystem.exists(part0offset2));
        assertEquals(String.format("payloadD%n"), readFullyAsString(part0offset2, 20));

        Path part1offset0 = new Path(result, "topic01/1/topic01-1-0000000000000000000");
        assertTrue(localFileSystem.exists(part1offset0));
        assertEquals(String.format("payloadB%n"), readFullyAsString(part1offset0, 20));

        Path part1offset1 = new Path(result, "topic01/1/topic01-1-0000000000000000001");
        assertTrue(localFileSystem.exists(part1offset1));
        assertEquals(String.format("payloadE%n"), readFullyAsString(part1offset1, 20));

    }


}
