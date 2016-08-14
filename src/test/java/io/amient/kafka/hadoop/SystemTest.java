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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
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
    private ZkClient zkClient;
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
        conf = new HdfsConfiguration();
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
        kafkaConnect =  "localhost:" + kafkaPort;
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


    private Path runTestJob(String topic, String testOutputDir) throws InterruptedException, IOException, ClassNotFoundException {

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
    public void canFollowKafkaPartitions() throws IOException, ClassNotFoundException, InterruptedException {

        //produce some data
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadA"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key2", "payloadB"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadC"));

        //run the first job
        runTestJob("topic01", "canFollowKafkaPartitions");

        //produce more data
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "payloadD"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key2", "payloadE"));

        //run the second job
        Path result = runTestJob("topic01", "canFollowKafkaPartitions");

        //check results
        Path part0offset0 = new Path(result, "topic01/0/0");
        assertTrue(localFileSystem.exists(part0offset0));
        try(FSDataInputStream out = localFileSystem.open(part0offset0)) {
            assertEquals("payloadA", out.readLine());
            assertEquals("payloadC", out.readLine());
        }
        Path part0offset2 = new Path(result, "topic01/0/2");
        assertTrue(localFileSystem.exists(part0offset2));
        try(FSDataInputStream out = localFileSystem.open(part0offset2)) {
            assertEquals("payloadD", out.readLine());
        }
        Path part1offset0 = new Path(result, "topic01/1/0");
        assertTrue(localFileSystem.exists(part1offset0));
        try(FSDataInputStream out = localFileSystem.open(part1offset0)) {
            assertEquals("payloadB", out.readLine());
        }
        Path part1offset1 = new Path(result, "topic01/1/1");
        assertTrue(localFileSystem.exists(part1offset1));
        try(FSDataInputStream out = localFileSystem.open(part1offset1)) {
            assertEquals("payloadE", out.readLine());
        }

    }

    //TODO path formatting with json timestamp extraction example


}
