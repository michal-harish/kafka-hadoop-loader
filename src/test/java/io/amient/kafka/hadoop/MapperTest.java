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
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapperTest {

    @Test
    public void testCanMapEmptyMessage() throws IOException {
        MessageMetadataWritable mockMessageSourceWritable = mock(MessageMetadataWritable.class);
        BytesWritable mockBytesWritable = mock(BytesWritable.class);

        when(mockBytesWritable.getBytes()).thenReturn(new byte[0]);
        when(mockMessageSourceWritable.toString()).thenReturn("Mock Metadata Summary");

        HadoopJobMapper mapper = new HadoopJobMapper();
        assertNull(mapper.map(mockMessageSourceWritable, mockBytesWritable));
    }

    @Test
    public void canCopySimpleTextMessage() throws IOException {
        String messageText = "test payload\u0000\u0000\u0000\u0000\u0000\u0000";

        MessageMetadataWritable mockMessageSourceWritable = mock(MessageMetadataWritable.class);
        when(mockMessageSourceWritable.getTopic()).thenReturn("topic1");
        when(mockMessageSourceWritable.toString()).thenReturn("Mock Metadata Summary");
        BytesWritable inputValue = new BytesWritable(messageText.getBytes());

        HadoopJobMapper mapper = new HadoopJobMapper();
        BytesWritable mapped = mapper.map(mockMessageSourceWritable, inputValue);
        assertEquals(messageText, new String(mapped.getBytes()));
    }
}
