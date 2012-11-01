package net.imagini.kafka.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaInputRecordReader extends RecordReader<LongWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private KafkaInputContext kcontext;
    private KafkaInputSplit ksplit;
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
        ksplit = (KafkaInputSplit) split;

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
            ksplit.getWatermark(),
            fsize, 
            timeout, 
            bsize, 
            reset
        );
        startOffset = kcontext.getStartOffset();
        endOffset = kcontext.getLastOffset();
        log.info(
            "JobId {} {} Start: {} End: {}", 
            new Object[]{context.getJobID(), ksplit, startOffset, endOffset }
        );
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
            long newOffset = kcontext.getNext(key, value);
            if (newOffset > 0) {
                currentOffsetPos = newOffset;
                numProcessedMessages++;
                return true;
            }
        }
        log.info("Next Offset " + currentOffsetPos);
        return false;
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
    public void close() throws IOException
    {
        kcontext.close();
        if (numProcessedMessages == 0L) return;

        Configuration conf = context.getConfiguration();
        ZkUtils zk = new ZkUtils(conf);
        String group = conf.get("kafka.groupid");
        String partition = ksplit.getBrokerId() + "-" + ksplit.getPartition();
        zk.commitLastConsumedOffset(group, ksplit.getTopic(), partition, currentOffsetPos);
        zk.close();
    }

}
