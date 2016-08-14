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

import io.amient.kafka.hadoop.io.KafkaInputSplit;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MapperTest {

    final private String jobId = "test-hadoop-job";
    private MapDriver<MsgMetadataWritable, BytesWritable, MsgMetadataWritable, BytesWritable> mapDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new HadoopJobMapper());
    }

    @Test
    public void testCanMapEmptyPayload() throws IOException {
        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "topic1", 3, 1234567890L);
        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getStartOffset());
        BytesWritable inputValue = new BytesWritable(new byte[0]);
        mapDriver.withInput(inputKey, inputValue);
        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
        BytesWritable value = result.get(0).getSecond();
        assertNotNull(value);
        assertEquals(0, value.copyBytes().length);
    }

    @Test
    public void canCopySimpleTextMessage() throws IOException {
        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "texttopic", 3, 1234567890L);
        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getStartOffset());

        String messageText = "some message with wierd payload\u0000\u0000\u0000\u0000\u0000\u0000";
        BytesWritable inputValue = new BytesWritable(messageText.getBytes());
        mapDriver.withInput(inputKey, inputValue);

        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
        assertEquals(1, result.size());
        assertEquals(split, result.get(0).getFirst().getSplit());
        assertEquals(messageText, new String(result.get(0).getSecond().copyBytes()));
    }



//    public static class JsonToMap extends HadoopJobMapper.Serde {
//        ObjectMapper jsonMapper = new ObjectMapper();
//        @Override
//        public BytesWritable map(BytesWritable value) throws IOException {
//            JsonNode x = jsonMapper.readValue(value.getBytes(), JsonNode.class);
//            MapWritable out = new MapWritable();
//            out.put(new Text("version"), new IntWritable(x.get("version").getIntValue()));
//            out.put(new Text("timestamp"), new LongWritable(x.get("timestamp").getLongValue()));
//            out.put(new Text("date"), new Text(x.get("date").getTextValue()));
//            out.put(new Text("utc"), new LongWritable(x.get("utc").getLongValue()));
//            return out;
//        }
//    }

//    @Test
//    public void testSomeJsonToCSVTransformation() throws IOException {
//        Configuration conf = mapDriver.getConfiguration();
//        KafkaInputFormat.configureKafkaTopics(conf, "jsontopic");
//        HadoopJobMapper.configureSerde(conf, JsonToMap.class.getName());
//
//        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "jsontopic", 3, 1234567890L);
//        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getWatermark() + 1);
//
//        String json = "{\"version\":5,\"timestamp\":1402944501425,\"date\":\"2014-06-16\",\"utc\":1402944501}";
//        byte[] jsonBytes = json.getBytes();
//        BytesWritable inputValue = new BytesWritable(jsonBytes, jsonBytes.length);
//
//
////        ArgumentCaptor<DataOutput> argument = ArgumentCaptor.forClass(DataOutput.class);
////        Mockito.doCallRealMethod().when(mockKey).write(argument.capture());
//
//        mapDriver.withInput(inputKey, inputValue);
//
//        // Execute test
//        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
//        String line0 = new String(toByteArray(result.get(0).getSecond()));
//        System.out.println(line0);
//        assertEquals("5\t1402944501425\t2014-06-16\t1402944501", line0);
//
////        DataOutputStream out = new DataOutputStream(System.out);
////        out.f
////        for (Pair<MsgMetadataWritable, BytesWritable> p : result) {
////            p.getFirst().write(out);
////            p.getSecond().write(out);
////        }
//    }

}
