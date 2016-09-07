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

package io.amient.kafka.hadoop.io;

import io.amient.kafka.hadoop.HadoopJobMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.TreeMap;

public class MultiOutputFormat extends FileOutputFormat<MsgMetadataWritable, BytesWritable> {

    final private static Logger log = LoggerFactory.getLogger(MultiOutputFormat.class);

    private static final String CONFIG_PATH_FORMAT = "multioutput.path.format";

    /**
     * @param format relative path format, e.g. 'topic={T}/d='yyyy-MM-dd'/h='HH'/{P}'
     */
    public static void configurePathFormat(Configuration conf, String format) {
        conf.set(CONFIG_PATH_FORMAT, format);
    }


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

    public RecordWriter<MsgMetadataWritable, BytesWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException {

        final TaskAttemptContext taskContext = context;
        final Configuration conf = context.getConfiguration();
        final boolean isCompressed = getCompressOutput(context);
        String ext = "";
        CompressionCodec gzipCodec = null;
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
            gzipCodec = ReflectionUtils.newInstance(codecClass, conf);
            ext = ".gz";
        }
        final CompressionCodec codec = gzipCodec;
        final String extension = ext;

        final String pathFormat = conf.get(CONFIG_PATH_FORMAT, "'{T}/{P}'");
        log.info("Using path format: " + pathFormat);
        final SimpleDateFormat timeFormat = new SimpleDateFormat(pathFormat);
        timeFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        final DecimalFormat offsetFormat = new DecimalFormat("0000000000000000000");
        final boolean hasTS = HadoopJobMapper.isTimestampExtractorConfigured(conf);

        return new RecordWriter<MsgMetadataWritable, BytesWritable>() {
            TreeMap<String, RecordWriter<Void, BytesWritable>> recordWriters = new TreeMap<>();

            Path prefixPath = ((FileOutputCommitter) getOutputCommitter(taskContext)).getWorkPath();

            public void write(MsgMetadataWritable key, BytesWritable value) throws IOException {
                if (hasTS && key.getTimestamp() == null) {
                    //extractor didn't wish to throw exception so skipping this record
                    return;
                }
                String P = String.valueOf(key.getSplit().getPartition());
                String T = key.getSplit().getTopic();
                String suffixPath = hasTS ? timeFormat.format(key.getTimestamp()) : pathFormat.replaceAll("'", "");
                suffixPath = suffixPath.replace("{T}", T);
                suffixPath = suffixPath.replace("{P}", P);
                suffixPath += "/" + T + "-"+ P + "-" + offsetFormat.format(key.getSplit().getStartOffset());
                suffixPath += extension;
                RecordWriter<Void, BytesWritable> rw = this.recordWriters.get(suffixPath);
                try {
                    if (rw == null) {
                        Path file = new Path(prefixPath, suffixPath);
                        FileSystem fs = file.getFileSystem(conf);
                        FSDataOutputStream fileOut = fs.create(file, false);
                        if (isCompressed) {
                            rw = new LineRecordWriter(new DataOutputStream(codec.createOutputStream(fileOut)));
                        } else {
                            rw = new LineRecordWriter(fileOut);
                        }
                        this.recordWriters.put(suffixPath, rw);
                    }
                    rw.write(null, value);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                Iterator<String> keys = this.recordWriters.keySet().iterator();
                while (keys.hasNext()) {
                    RecordWriter<Void, BytesWritable> rw = this.recordWriters.get(keys.next());
                    rw.close(context);
                }
                this.recordWriters.clear();
            }

        };
    }

    protected static class LineRecordWriter extends RecordWriter<Void, BytesWritable> {

        private static final byte[] newline = String.format("%n").getBytes();

        protected DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        public synchronized void write(Void key, BytesWritable value)
                throws IOException {
            out.write(value.getBytes(), 0, value.getLength());
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
