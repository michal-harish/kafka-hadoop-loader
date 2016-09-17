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

package io.amient.kafka.hadoop.testutils;

import io.amient.kafka.hadoop.HadoopJobMapper;
import io.amient.kafka.hadoop.KafkaZkUtils;
import io.amient.kafka.hadoop.TimestampExtractorSystemTest;
import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MultiOutputFormat;
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
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
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class SystemTestBase {

    protected Configuration conf;
    protected Producer<String, String> simpleProducer;
    protected LocalFileSystem localFileSystem;
    protected String zkConnect;
    protected String kafkaBootstrap;

    private File dfsBaseDir;
    private File embeddedClusterPath;
    private File embeddedZkPath;
    private File embeddedKafkaPath;
    private MiniDFSCluster cluster;
    private FileSystem fs;
    private ZooKeeperServer zookeeper;
    private NIOServerCnxnFactory zkFactory;
    private KafkaServerStartable kafka;

    @Before
    public void setUp() throws IOException, InterruptedException {
        dfsBaseDir = new File(TimestampExtractorSystemTest.class.getResource("/systemtest").getPath());

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
        System.out.println("starting local kafka broker...");

        embeddedKafkaPath = new File(dfsBaseDir, "local-kafka-logs");
        KafkaConfig kafkaConfig = new KafkaConfig(new Properties() {{
            put("broker.id", "1");
            put("host.name", "localhost");
            put("port", "0");
            put("log.dir", embeddedKafkaPath.toString());
            put("num.partitions", "2");
            put("auto.create.topics.enable", "true");
            put("zookeeper.connect", zkConnect);
        }});
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();

        //dynamic kafka port allocation
        try(KafkaZkUtils tmpZkClient = new KafkaZkUtils(zkConnect, 30000, 6000)) {
            Broker broker = Broker.createBroker(1, tmpZkClient.getBrokerInfo(1));
            kafkaBootstrap = broker.getBrokerEndPoint(SecurityProtocol.PLAINTEXT).connectionString();
        }

        System.out.println("preparing simpleProducer..");
        simpleProducer = new Producer<>(new ProducerConfig(new Properties() {{
            put("metadata.broker.list", kafkaBootstrap);
            put("serializer.class", "kafka.serializer.StringEncoder");
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


    protected String readFullyAsString(Path file, int maxSize) throws IOException {
        try (FSDataInputStream in = localFileSystem.open(file)) {
            byte[] bytes = new byte[Math.min(in.available(), maxSize)];
            in.readFully(bytes);
            return new String(bytes);
        }
    }


    protected Path runSimpleJob(String topic, String testOutputDir)
            throws InterruptedException, IOException, ClassNotFoundException {

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
        if (!job.isSuccessful()) throw new Error("job failed - see logs for details");

        fs.copyToLocalFile(outDir, outDir);
        return outDir;
    }

}
