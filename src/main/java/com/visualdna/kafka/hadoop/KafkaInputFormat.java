package com.visualdna.kafka.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    static Logger LOG = LoggerFactory.getLogger(KafkaInputFormat.class);

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        return new KafkaRecordReader() ;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ZkUtils zk = new ZkUtils(conf);
        String[] topics = conf.get("kafka.topics").split(",");
        String group = conf.get("kafka.groupid");
        List<InputSplit> splits = new ArrayList<InputSplit>();
        for(String topic: topics)
        {
            List<String> partitions = zk.getPartitions(topic);
            for(String partition: partitions) {
                String[] sp = partition.split("-");
                long last = zk.getLastCommit(group, topic, partition) ;
                InputSplit split = new KafkaSplit(sp[0], zk.getBroker(sp[0]), topic, Integer.valueOf(sp[1]), last);
                splits.add(split);
            }
        }
        zk.close();
        return splits;
    }

    public static class KafkaSplit extends InputSplit implements Writable {

        private String brokerId;
        private String broker;
        private int partition;
        private String topic;
        private long lastCommit;

        public KafkaSplit() {}

        public KafkaSplit(String brokerId, String broker, String topic, int partition, long lastCommit) {
            this.brokerId = brokerId;
            this.broker = broker;
            this.partition = partition;
            this.topic = topic;
            this.lastCommit = lastCommit;
        }

        public void readFields(DataInput in) throws IOException {
            brokerId = Text.readString(in);
            broker = Text.readString(in);
            topic = Text.readString(in);
            partition = in.readInt();
            lastCommit = in.readLong();
        }

        public void write(DataOutput out) throws IOException {
            Text.writeString(out, brokerId);
            Text.writeString(out, broker);
            Text.writeString(out, topic);
            out.writeInt(partition);
            out.writeLong(lastCommit);
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return Long.MAX_VALUE;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[] {broker};
        }

        public String getBrokerId() {
            return brokerId;
        }

        public String getBroker() {
            return broker;
        }

        public int getPartition() {
            return partition;
        }

        public String getTopic() {
            return topic;
        }

        public long getLastCommit() {
            return lastCommit;
        }

        @Override
        public String toString() {
            return broker + "-" + topic + "-" + partition + "-" + lastCommit ;
        }
    }

    public static class KafkaRecordReader extends RecordReader<LongWritable, BytesWritable> {

        private KafkaInputContext kcontext;
        private KafkaSplit ksplit;
        private TaskAttemptContext context;
        private int maxProcessMessages;
        private LongWritable key;
        private BytesWritable value;
        private long startOffset;
        private long endOffset;
        private long currentOffsetPos;
        private long numProcessedMessages = 0L;
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException 
        {
            this.context = context;
            ksplit = (KafkaSplit) split;

            Configuration conf = context.getConfiguration();
            maxProcessMessages = conf.getInt("kafka.limit", -1);

            int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
            int bsize = conf.getInt("kafka.socket.buffersize", 64*1024);
            int fsize = conf.getInt("kafka.fetch.size", 1024 * 1024);
            String reset = conf.get("kafka.autooffset.reset");
            kcontext = new KafkaInputContext(
                ksplit.getBrokerId() + ":" + ksplit.getBroker(),
                ksplit.getTopic(), 
                ksplit.getPartition(),
                ksplit.getLastCommit(),
                fsize, 
                timeout, 
                bsize, 
                reset
            );
            startOffset = kcontext.getStartOffset();
            endOffset = kcontext.getLastOffset();
            LOG.info(
                "JobId {} {} Start: {} End: {}", 
                new Object[]{context.getJobID(), ksplit, startOffset, endOffset }
            );
        }

        @Override
        public void close() throws IOException
        {
            kcontext.close();
            commit();
        }

        private void commit() throws IOException
        {
            if (numProcessedMessages == 0L) return;
            Configuration conf = context.getConfiguration();
            ZkUtils zk = new ZkUtils(conf);
            String group = conf.get("kafka.groupid");
            String partition = ksplit.getBrokerId() + "-" + ksplit.getPartition();
            //commit temporary offset
            zk.setLastCommit(group, ksplit.getTopic(), partition, currentOffsetPos, true);
            zk.close();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException
        {
            return key;
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException
        {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException 
        {
            if (currentOffsetPos >= endOffset || startOffset == endOffset) {
                return 1.0f;
            }

            if (maxProcessMessages < 0) {
                return Math.min(1.0f, (currentOffsetPos - startOffset) / (float)(endOffset - startOffset));
            } else {
                return Math.min(1.0f, numProcessedMessages / (float)maxProcessMessages);
            }
        }


        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException
        {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new BytesWritable();
            }
            if (maxProcessMessages < 0 || numProcessedMessages < maxProcessMessages) {
                long watermark = kcontext.getNext(key, value);
                if (watermark > 0) {
                    currentOffsetPos = watermark;
                    numProcessedMessages++;
                    return true;
                }
            }
            LOG.info("Next Offset " + currentOffsetPos);
            return false;
        }
    }

}
