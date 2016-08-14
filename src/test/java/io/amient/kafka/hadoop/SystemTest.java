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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SystemTest {

    private File dfsBaseDir;
    private File embeddedClusterPath;
    private File embeddedZkPath;
    private File embeddedKafkaPath;
    private Configuration conf;
    private MiniDFSCluster cluster;
    private FileSystem fs;
    private ZooKeeperServer zookeeper;
    private NIOServerCnxnFactory zkFactory;
    private KafkaServerStartable kafka;
    private String zkConnect;
    private String kafkaConnect;
    private Producer<String, String> simpleProducer;
    private LocalFileSystem localFileSystem;


    @Before
    public void setUp() throws IOException, InterruptedException {
        dfsBaseDir = new File(SystemTest.class.getResource("/systemtest").getPath());

        //setup hadoop node
        embeddedClusterPath = new File(dfsBaseDir, "local-cluster");
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, embeddedClusterPath.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = FileSystem.get(conf);
        localFileSystem = FileSystem.getLocal(conf);

        //setup zookeeper
        embeddedZkPath = new File(dfsBaseDir, "local-zookeeper");
        zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000);
        zkFactory = new NIOServerCnxnFactory();
        zkFactory.configure(new InetSocketAddress(0), 10);
        zkConnect = "localhost:" + zkFactory.getLocalPort();
        System.out.println("starting local zookeeper at " + zkConnect);
        zkFactory.startup(zookeeper);

        //setup kafka
        final String kafkaPort = "9092"; //FIXME dynamic port allocation, otherwise this may break the build
        kafkaConnect = "localhost:" + kafkaPort;
        System.out.println("starting local kafka broker...");
        embeddedKafkaPath = new File(dfsBaseDir, "local-kafka-logs");
        KafkaConfig kafkaConfig = new KafkaConfig(new Properties() {{
            put("broker.id", "1");
            put("host.name", "localhost");
            put("port", kafkaPort);
            put("log.dir", embeddedKafkaPath.toString());
            put("num.partitions", "2");
            put("auto.create.topics.enable", "true");
            put("zookeeper.connect", zkConnect);
        }});
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();

        System.out.println("preparing simpleProducer..");
        simpleProducer = new Producer<>(new ProducerConfig(new Properties() {{
            put("metadata.broker.list", kafkaConnect);
            put("serializer.class", "kafka.serializer.StringEncoder");
            //put("partitioner.class", "example.simpleProducer.SimplePartitioner");
            put("request.required.acks", "1");
        }}));

        System.out.println("system test setup complete");

    }

    @After
    public void tearDown() throws Exception {
        try {
            try {
                simpleProducer.close();
                System.out.println("Shutting down kafka...");
                try {
                    kafka.shutdown();
                } catch (IllegalStateException e) {
                    //
                }
                System.out.println("Shutting down zookeeper...");
                zkFactory.shutdown();
            } finally {
                System.out.println("Shutting down dfs cluster...");
                cluster.shutdown();
            }
        } finally {
            System.out.println("Cleaning up directories...");
            localFileSystem.delete(new Path(embeddedClusterPath.toString()), true);
            localFileSystem.delete(new Path(embeddedZkPath.toString()), true);
            localFileSystem.delete(new Path(embeddedKafkaPath.toString()), true);

        }
    }


    private Path runSimpleJob(String topic, String testOutputDir) throws InterruptedException, IOException, ClassNotFoundException {

        //run hadoop loader job
        Path outDir = new Path(new File(dfsBaseDir, testOutputDir).toString());
        localFileSystem.delete(outDir, true);

        KafkaInputFormat.configureKafkaTopics(conf, topic);
        KafkaInputFormat.configureZkConnection(conf, zkConnect);

        Job job = Job.getInstance(conf, "kafka.hadoop.loader");
        job.setNumReduceTasks(0);

        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);

        MultiOutputFormat.setOutputPath(job, outDir);
        MultiOutputFormat.setCompressOutput(job, false);

        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());

        fs.copyToLocalFile(outDir, outDir);
        return outDir;
    }

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

    @Test
    public void canUseTimestampInPartitions() throws IOException, ClassNotFoundException, InterruptedException {

        //produce some json data
        String message5 = "{\"version\":5,\"timestamp\":1402944501425,\"id\": 1}";
        simpleProducer.send(new KeyedMessage<>("topic02", "1", message5));
        String message1 = "{\"version\":1,\"timestamp\":1402945801425,\"id\": 2}";
        simpleProducer.send(new KeyedMessage<>("topic02", "2", message1));
        String message6 = "{\"version\":6,\"timestamp\":1402948801425,\"id\": 1}";
        simpleProducer.send(new KeyedMessage<>("topic02", "1", message6));

        //run the job
        Path outDir = new Path(new File(dfsBaseDir, "canUseTimestampInPartitions").toString());
        localFileSystem.delete(outDir, true);

        //configure inputs, timestamp extractor and the output path format
        KafkaInputFormat.configureKafkaTopics(conf, "topic02");
        KafkaInputFormat.configureZkConnection(conf, zkConnect);
        HadoopJobMapper.configureTimestampExtractor(conf, MyJsonTimestampExtractor.class.getName());
        MultiOutputFormat.configurePathFormat(conf, "'t={T}/d='yyyy-MM-dd'/h='HH");

        Job job = Job.getInstance(conf, "kafka.hadoop.loader");
        job.setNumReduceTasks(0);

        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);

        MultiOutputFormat.setOutputPath(job, outDir);
        MultiOutputFormat.setCompressOutput(job, false);

        job.waitForCompletion(true);

        assertTrue(job.isSuccessful());

        fs.copyToLocalFile(outDir, outDir);

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

    private String readFullyAsString(Path file, int maxSize) throws IOException {
        try (FSDataInputStream in = localFileSystem.open(file)) {
            byte[] bytes = new byte[Math.min(in.available(), maxSize)];
            in.readFully(bytes);
            return new String(bytes);
        }
    }


}
