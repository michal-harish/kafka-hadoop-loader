/*
 * Copyright 2012 Michal Harish, michal.harish@gmail.com
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

import io.amient.kafka.hadoop.format.KafkaInputFormat;
import io.amient.kafka.hadoop.format.KafkaInputSplit;
import io.amient.kafka.hadoop.format.KafkaOutputFormat;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class HadoopJob extends Configured implements Tool {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(HadoopJob.class);

    static {
        Configuration.addDefaultResource("core-site.xml");
    }

    public int run(String[] args) throws Exception {
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h") || cmd.getArgs().length == 0) {
            printHelpAndExit(options);
        }

        String hdfsPath = cmd.getArgs()[0];
        Configuration conf = getConf();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);

        String[] topics;
        if (cmd.hasOption("topics")) {
            conf.set("kafka.topics", cmd.getOptionValue("topics"));
            LOG.info("Using topics: " + conf.get("kafka.topics"));
            topics = cmd.getOptionValue("topics").split(",");
            conf.setStrings("vdna.khl.topics", topics);
        } else if (cmd.hasOption("filter")) {
            conf.set("kafka.topic.filter", cmd.getOptionValue("filter"));
            LOG.info("Using topic filter: " + conf.get("kafka.topic.filter"));
            throw new Exception("Topic filter not implemented");
        } else {
            printHelpAndExit(options);
        }

        conf.set("kafka.group.id", cmd.getOptionValue("consumer-group", "dev-hadoop-loader"));
        LOG.info("Registering under consumer group: " + conf.get("kafka.group.id"));

        conf.set("kafka.zookeeper.connect", cmd.getOptionValue("zk-connect", "localhost:2181"));

        LOG.info("Using ZooKeeper connection: " + conf.get("kafka.zookeeper.connect"));

        if (cmd.getOptionValue("autooffset-reset") != null) {
            conf.set("kafka.watermark.reset", cmd.getOptionValue("autooffset-reset"));
            LOG.info("SHOULD RESET OFFSET TO: " + conf.get("kafka.watermark.reset"));
        }

        JobConf jobConf = new JobConf(conf);
        if (cmd.hasOption("remote")) {
            String ip = cmd.getOptionValue("remote");
            LOG.info("Default file system: hdfs://" + ip + ":8020/");
            jobConf.set("fs.defaultFS", "hdfs://" + ip + ":8020/");
            LOG.info("Remote jobtracker: " + ip + ":8021");
            jobConf.set("mapred.job.tracker", ip + ":8021");
        }

        Path jarTarget = new Path(
                getClass().getProtectionDomain().getCodeSource().getLocation()
                        + "../kafka-hadoop-loader.jar"
        );

        if (new File(jarTarget.toUri()).exists()) {
            // running from eclipse / as maven
            jobConf.setJar(jarTarget.toUri().getPath());
            LOG.info("Using target jar: " + jarTarget.toString());
        } else {
            // running from jar remotely or locally
            jobConf.setJarByClass(getClass());
            LOG.info("Using parent jar: " + jobConf.getJar());
        }

        // determine all partitions + leaders ... save to config/job context
        // in the KIF -> load data from job context.config and return splits

        KafkaZkUtils zk = new KafkaZkUtils(
                conf.get("kafka.zookeeper.connect"),
                conf.getInt("kafka.zookeeper.session.timeout.ms", 10000),
                conf.getInt("kafka.zookeeper.connection.timeout.ms", 10000)
        );

        boolean success = false;

        try {
            Job job = Job.getInstance(jobConf, "kafka.hadoop.loader");

            List<KafkaInputSplit> splits = KafkaInputSplitsBuilder.createInputSplitPerPartitionLeader(zk, conf);
            DefaultStringifier.storeArray(job.getConfiguration(), splits.toArray(), KafkaInputSplit.KAFKA_INPUT_SPLIT_CONF_KEY);

            job.setInputFormatClass(KafkaInputFormat.class);
            job.setMapperClass(HadoopJobMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setOutputFormatClass(KafkaOutputFormat.class);
            job.setNumReduceTasks(0);

            KafkaOutputFormat.setOutputPath(job, new Path(hdfsPath));
            KafkaOutputFormat.setCompressOutput(job, cmd.getOptionValue("compress-output", "on").equals("on"));

            LOG.info("Output hdfs location: {}", hdfsPath);
            LOG.info("Output hdfs compression: {}", KafkaOutputFormat.getCompressOutput(job));

            success = job.waitForCompletion(true);
        } finally {
            zk.close();
        }

        return success ? 0 : -1;
    }

    private void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("kafka-hadoop-loader.jar", options);
        System.exit(0);
    }

    @SuppressWarnings("static-access")
    private Options buildOptions() {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("topics")
                .withLongOpt("topics")
                .hasArg()
                .withDescription("kafka topics")
                .create("t"));

        options.addOption(OptionBuilder.withArgName("groupid")
                .withLongOpt("consumer-group")
                .hasArg()
                .withDescription("kafka consumer groupid")
                .create("g"));

        options.addOption(OptionBuilder.withArgName("zk")
                .withLongOpt("zk-connect")
                .hasArg()
                .withDescription("ZooKeeper connection String")
                .create("z"));

        options.addOption(OptionBuilder.withArgName("offset")
                .withLongOpt("offset-reset")
                .hasArg()
                .withDescription("Reset all offsets to either 'earliest' or 'latest'")
                .create("o"));

        options.addOption(OptionBuilder.withArgName("compression")
                .withLongOpt("compress-output")
                .hasArg()
                .withDescription("GZip output compression on|off")
                .create("c"));

        options.addOption(OptionBuilder.withArgName("ip_address")
                .withLongOpt("remote")
                .hasArg()
                .withDescription("Running on a remote hadoop node")
                .create("r"));

        options.addOption(OptionBuilder
                .withLongOpt("help")
                .withDescription("Show this help")
                .create("h"));

        options.addOption(OptionBuilder.withArgName("topic_filter")
                .withLongOpt("filter")
                .hasArg()
                .withDescription("Topic filter")
                .create("f"));
        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopJob(), args);
        System.exit(exitCode);
    }

}
