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
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class FormatterTest {

    MapDriver<MessageMetadataWritable, BytesWritable, MessageMetadataWritable, BytesWritable> mapDriver;

    @Before
    public void setUp() {
        HadoopJobMapper mapper = new HadoopJobMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        String topic = "topic1";
        String date = "2014-06-16";
        String brokerHost = "host-01";
        int broker = 1;
        int partition = 3;
        long offset = 1234567890L;

        String json = "{\"version\":5,\"timestamp\":1402944501425,\"date\":\""+date+"\",\"utc\":1402944501}";
        byte[] jsonBytes = json.getBytes();

        BytesWritable realValue = new BytesWritable(jsonBytes, jsonBytes.length);

        MessageMetadataWritable mockKey = mock(MessageMetadataWritable.class);
        when(mockKey.getBrokerId()).thenReturn(broker);
        when(mockKey.getPartition()).thenReturn(partition);
        when(mockKey.getTopic()).thenReturn(topic);
        when(mockKey.getBrokerHost()).thenReturn(brokerHost);
        when(mockKey.getOffset()).thenReturn(offset);

        ArgumentCaptor<DataOutput> argument = ArgumentCaptor.forClass(DataOutput.class);
        Mockito.doCallRealMethod().when(mockKey).write(argument.capture());

        mapDriver.withInput(mockKey, realValue);

        // Execute test
        final List<Pair<MessageMetadataWritable, BytesWritable>> result = mapDriver.run();
//        assertEquals(result.get(0).getFirst().toString(), date);
//        assertEquals(result.get(0).getSecond().getDataAsString(), json);
    }
}
