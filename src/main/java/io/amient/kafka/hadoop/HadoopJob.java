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

import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MultiOutputFormat;
import org.apache.commons.cli.*;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.LoggerFactory;

import java.io.File;

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

        if (cmd.hasOption("topics")) {
            LOG.info("Using topics: " + cmd.getOptionValue("topics"));
            KafkaInputFormat.configureKafkaTopics(conf, cmd.getOptionValue("topics"));
        } else if (cmd.hasOption("filter")) {
            LOG.info("Using topic filter: " + cmd.getOptionValue("filter"));
            //TODO KafkaInputFormat.configureTopicFilter(conf, cmd.getOptionValue("filter"));
            throw new NotImplementedException("Topic filter not implemented");
        } else {
            printHelpAndExit(options);
        }

        KafkaInputFormat.configureGroupId(conf, cmd.getOptionValue("consumer-group", "dev-hadoop-loader"));
        KafkaInputFormat.configureZkConnection(conf, cmd.getOptionValue("zk-connect", "localhost:2181"));

        if (cmd.getOptionValue("autooffset-reset") != null) {
            KafkaInputFormat.configureAutoOffsetReset(conf, cmd.getOptionValue("autooffset-reset"));
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
            // running from IDE/ as maven
            jobConf.setJar(jarTarget.toUri().getPath());
            LOG.info("Using target jar: " + jarTarget.toString());
        } else {
            // running from jar remotely or locally
            jobConf.setJarByClass(getClass());
            LOG.info("Using parent jar: " + jobConf.getJar());
        }


        Job job = Job.getInstance(jobConf, "kafka.hadoop.loader");

        job.setInputFormatClass(KafkaInputFormat.class);
        job.setMapperClass(HadoopJobMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(MultiOutputFormat.class);
        job.setNumReduceTasks(0);

        MultiOutputFormat.setOutputPath(job, new Path(hdfsPath));
        MultiOutputFormat.setCompressOutput(job, cmd.getOptionValue("compress-output", "on").equals("on"));

        LOG.info("Output hdfs location: {}", hdfsPath);
        LOG.info("Output hdfs compression: {}", MultiOutputFormat.getCompressOutput(job));

        return job.waitForCompletion(true) ? 0 : -1;
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
