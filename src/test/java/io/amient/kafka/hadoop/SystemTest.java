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
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.server.KafkaServerStartable;
import org.I0Itec.zkclient.ZkClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class SystemTest {

    private File testDataPath;
    private File embeddedClusterPath;
    private File embeddedZkPath;
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


    @Before
    public void setUp() throws IOException, InterruptedException {
        testDataPath = new File(SystemTest.class.getResource("/minidfs").getPath());

        embeddedClusterPath = new File(testDataPath, "local-cluster");
        System.out.println(embeddedClusterPath);
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, embeddedClusterPath.getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(conf).build();
        fs = FileSystem.get(conf);



        embeddedZkPath = new File(testDataPath, "local-zookeeper");
        zookeeper = new ZooKeeperServer(new File(embeddedZkPath, "snapshots"), new File(embeddedZkPath, "logs"), 3000);
        zkFactory = new NIOServerCnxnFactory();
        zkFactory.configure(new InetSocketAddress(0), 1);
        zkConnect = "localhost:" + zkFactory.getLocalPort();
        System.out.println("starting local zookeeper at " + zkConnect);
        zkFactory.startup(zookeeper);
        //zkClient = new ZkClient();
        //zkClient.setZkSerializer(StringSerializer)

        final String kafkaPort = "9092"; //TODO dynamic port allocation
        kafkaConnect =  "localhost:" + kafkaPort;
        System.out.println("starting local kafka broker...");
        KafkaConfig kafkaConfig = new KafkaConfig(new Properties() {{
            put("broker.id", "0");
            put("host.name", "localhost");
            put("port", kafkaPort);
            put("log.dir", new File(testDataPath, "local-kafka-logs").toString());
            put("num.partitions", "4");
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
                kafka.shutdown();
                System.out.println("Shutting down zookeeper...");
                zkFactory.shutdown();
            } finally {
                System.out.println("Shutting down dfs cluster...");
                cluster.shutdown();
            }
        } finally {
//            System.out.println("Cleaning up directories...");
//            LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
//            localFileSystem.delete(new Path(embeddedClusterPath.toString()), true);
        }
    }

    @Test
    public void canFollowKafkaPartitions() throws IOException, ClassNotFoundException, InterruptedException {
        final String topic = "topic01";

        //produce some data
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, "key1", "value1");
        simpleProducer.send(data);

        //run hadoop loader job
        Path outDir = new Path(new File(testDataPath, "output").toString());
        fs.delete(outDir, true);

        KafkaInputFormat.configureKafkaTopics(conf, topic);
        KafkaInputFormat.configureZkConnection(conf, zkConnect);
        KafkaInputFormat.configureGroupId(conf, "test-loader");

        Job job = Job.getInstance(conf, "kafka.hadoop.loader");
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);

        MultiOutputFormat.setOutputPath(job, outDir);
        MultiOutputFormat.setCompressOutput(job, false);

        job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());

        //TODO check output
    }

}
