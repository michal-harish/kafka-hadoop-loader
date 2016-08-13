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

package io.amient.kafka.hadoop.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.TreeMap;

public class KafkaOutputFormat<K extends Writable, V extends Writable> extends FileOutputFormat<K, V> {
    public void checkOutputSpecs(JobContext job) throws IOException {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }

        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(
                job.getCredentials(),
                new Path[]{outDir},
                job.getConfiguration()
        );
    }

    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException {

        final TaskAttemptContext taskContext = context;
        final Configuration conf = context.getConfiguration();
        final boolean isCompressed = getCompressOutput(context);
        String ext = "";
        CompressionCodec gzipCodec = null;
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            gzipCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            ext = ".gz";
        }
        final CompressionCodec codec = gzipCodec;
        final String extension = ext;

        return new RecordWriter<K, V>() {
            TreeMap<String, RecordWriter<K, V>> recordWriters = new TreeMap<String, RecordWriter<K, V>>();

            public void write(K key, V value) throws IOException {

                String keyBasedPath = "d=" + key.toString();

                RecordWriter<K, V> rw = this.recordWriters.get(keyBasedPath);
                try {
                    if (rw == null) {
                        Path file = new Path(
                                ((FileOutputCommitter) getOutputCommitter(taskContext)).getWorkPath(),
                                getUniqueFile(
                                        taskContext, keyBasedPath + "/" + taskContext.getJobID().toString().replace("job_", ""),
                                        extension
                                )
                        );
                        FileSystem fs = file.getFileSystem(conf);
                        FSDataOutputStream fileOut = fs.create(file, false);
                        if (isCompressed) {
                            rw = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileOut)));
                        } else {
                            rw = new LineRecordWriter<K, V>(fileOut);
                        }
                        this.recordWriters.put(keyBasedPath, rw);
                    }
                    rw.write(key, value);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            ;

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                Iterator<String> keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter<K, V> rw = this.recordWriters.get(keys.next());
                    rw.close(context);
                }
                this.recordWriters.clear();
            }

            ;
        };
    }

    //TODO delegate to pluggable formatter whether it's line or binary etc
    protected static class LineRecordWriter<K extends Writable, V extends Writable>
            extends RecordWriter<K, V> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;

        static {
            try {
                newline = String.format("%n").getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        protected DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        public synchronized void write(K key, V value)
                throws IOException {

            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullValue) {
                return;
            }
            value.write(out);
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
