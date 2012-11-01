package com.visualdna.kafka.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import co.gridport.protos.VDNAEvent.Event;

import com.googlecode.protobuf.format.JsonFormat;

public class HadoopJob extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("core-site.xml");
        //Configuration.addDefaultResource("mapred-site.xml");
    }

    public static class KafkaMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
        @Override
        public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
            Text date = new Text();
            Text out = new Text();
            try {
                if (context.getConfiguration().get("input.format").equals("json"))
                {
                    //grab the raw json date
                    String json = new String(value.getBytes());
                    int i = json.indexOf(",\"date\":\"") + 9;
                    date.set(value.getBytes(), i, 10);
                    out.set(value.getBytes(),0, value.getLength());
                } else if (context.getConfiguration().get("input.format").equals("binary"))
                {
                    //Open the proto
                    Event event = Event.parseFrom(value.copyBytes());
                    date.set(event.getDate());
                    out.set( JsonFormat.printToString(event));
                }
                else
                {
                    throw new IOException("Invalid mapper input.format");
                }
                context.write(date, out);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public int run(String[] args) throws Exception {

        //parse options and arguments
        CommandLineParser parser = new PosixParser();
        Options options = buildOptions();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h") || cmd.getArgs().length == 0)
        {
           printHelpAndExit(options);
        }
        String hdfsPath = cmd.getArgs()[0];

        //initilialize configuration object
        Configuration conf = getConf();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);

        //configure the topic or topic filter
        if (cmd.hasOption("topics"))
        {
            conf.set("kafka.topics", cmd.getOptionValue("topics"));
            Logger.getRootLogger().info("Using topics: " + conf.get("kafka.topics"));
        }
        else if (cmd.hasOption("filter"))
        {
            conf.set("kafka.topic.filter", cmd.getOptionValue("filter"));
            Logger.getRootLogger().info("Using topic filter: " + conf.get("kafka.topic.filter"));
            throw new Exception("Topic filter not implemented");
        }
        else
        {
            //either topic or filter must be set
            printHelpAndExit(options);
        }

        //configure kafka consumer group 
        conf.set("kafka.groupid", cmd.getOptionValue("consumer-group", "dev-hadoop-loader"));
        Logger.getRootLogger().info("Registering under consumer group: " + conf.get("kafka.groupid")); 

        //configure zk connection for coordination work
        conf.set("kafka.zk.connect", cmd.getOptionValue("zk-connect", "hq-mharis-d02:2181"));
        Logger.getRootLogger().info("Using ZooKepper connection: " + conf.get("kafka.zk.connect"));

        //confgiure reset offset 
        if (cmd.getOptionValue("autooffset-reset") != null)
        {
            conf.set("kafka.autooffset.reset", cmd.getOptionValue("autooffset-reset"));
            Logger.getRootLogger().info("SHOULD RESET OFFSET TO: " + conf.get("kafka.autooffset.reset"));
        }

        //configure number messages default limit
        conf.setInt("kafka.limit", Integer.valueOf(cmd.getOptionValue("limit", "25000000")));
        Logger.getRootLogger().info("MAXIMUM MESSAGES PROCESSED BY THE JOB: " + conf.get("kafka.limit"));

        //configure input format
        conf.set("input.format", cmd.getOptionValue("input-format", "json"));
        if (!conf.get("input.format").equals("json") && !conf.get("input.format").equals("binary"))
        {
            printHelpAndExit(options);
        }
        Logger.getRootLogger().info("EXPECTING MESSAGE FORMAT: " + conf.get("input.format"));

        //Create the job configuration and set jar
        JobConf jobConf = new JobConf(conf);
        if (cmd.hasOption("remote") )
        {
            String ip = cmd.getOptionValue("remote");
            Logger.getRootLogger().info("Default file system: hdfs://" + ip + ":8020/");
            jobConf.set("fs.defaultFS", "hdfs://"+ip+":8020/");
            Logger.getRootLogger().info("Remote jobtracker: " + ip + ":8021");
            jobConf.set("mapred.job.tracker", ip+":8021");
        }
        Path jarTarget = new Path(
            getClass().getProtectionDomain().getCodeSource().getLocation()
            + "../kafka-hadoop-consumer-jar-with-dependencies.jar"
        );
        if (new File(jarTarget.toUri() ).exists())
        {
            //running from eclipse / as maven
            jobConf.setJar(jarTarget.toUri().getPath());
            Logger.getRootLogger().info("Using target jar: " + jarTarget.toString());
        }
        else
        {
            //running from jar remotely or locally
            jobConf.setJarByClass(getClass());
            Logger.getRootLogger().info("Using parent jar: " + jobConf.getJar());
        }

        //Launch the job
        Job job = new Job(jobConf, "kafka.hadoop.loader");
        job.setMapperClass(KafkaMapper.class);
        job.setInputFormatClass(KafkaInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);
        job.setNumReduceTasks(0);
        KafkaOutputFormat.setOutputPath(job, new Path(hdfsPath));
        KafkaOutputFormat.setCompressOutput(job, true);
        Logger.getRootLogger().info("Output hdfs location: " + hdfsPath);
        boolean success = job.waitForCompletion(true);
        if (success) {
            commit(conf);
        }
        return success ? 0: -1;
    }

    private void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "kafka.consumer.hadoop", options );        
        System.exit(0);
    }

    private void commit(Configuration conf) throws IOException {
        ZkUtils zk = new ZkUtils(conf);
        try {
            String group = conf.get("kafka.groupid");
            String[] topics = conf.get("kafka.topics").split(",");
            for(String topic: topics)
            {
                zk.commit(group, topic);
            }
        } catch (Exception e) {
            rollback();
        } finally {
            zk.close();
        }
    }

    private void rollback() {
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
                .withLongOpt("autooffset-reset")
                .hasArg()
                .withDescription("Offset reset")
                .create("o"));

        options.addOption(OptionBuilder.withArgName("limit")
                .withLongOpt("limit")
                .hasArg()
                .withDescription("Kafka limit (bytes)")
                .create("l"));

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

        options.addOption(OptionBuilder.withArgName("json|binary")
                .withLongOpt("input-format")
                .hasArg()
                .withDescription("How are the input messages formatted in the topic")
                .create("i"));
        return options;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HadoopJob(), args);
        System.exit(exitCode);
    }

}
