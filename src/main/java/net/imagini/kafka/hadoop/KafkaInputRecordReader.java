package net.imagini.kafka.hadoop;

import java.io.IOException;
import java.util.Iterator;

import kafka.api.FetchRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

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

    private Configuration conf;

    private KafkaInputSplit split;
    //private TaskAttemptContext context;

    private SimpleConsumer consumer ;
    private int fetchSize;
    private String topic;
    private String reset;

    private int partition;
    private long earliestOffset;
    private long watermark;
    private long latestOffset;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private LongWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException
    {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        topic = this.split.getTopic();
        partition = this.split.getPartition();
        watermark = this.split.getWatermark();

        int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
        int bufferSize = conf.getInt("kafka.socket.buffersize", 64*1024);
        consumer =  new SimpleConsumer(this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize);

        fetchSize = conf.getInt("kafka.fetch.size", 1024 * 1024);
        reset = conf.get("kafka.watermark.reset", "watermark");
        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        if ("earliest".equals(reset)) {
            resetWatermark(-1);
        } else if("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (watermark < earliestOffset) {
            resetWatermark(-1);
        }

        log.info(
            "Split {} Topic: {} Broker: {} Partition: {} Earliest: {} Latest: {} Starting: {}", 
            new Object[]{this.split, topic, this.split.getBrokerId(), partition, earliestOffset, latestOffset, watermark }
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

        if (messages == null) {
            FetchRequest request = new FetchRequest(topic, partition, watermark, fetchSize);
            log.info("fetching offset {} of topic {}", watermark, topic);
            messages = consumer.fetch(request);
            if (messages.getErrorCode() == ErrorMapping.OffsetOutOfRangeCode())
            {
                log.info("Out of bounds = " + watermark);
                return false;
            }
            if (messages.getErrorCode() != 0)
            {
                log.warn("Messages fetch error code: " + messages.getErrorCode());
                return false;
            } else {
                iterator = messages.iterator();
                watermark += messages.validBytes();
                if (!iterator.hasNext())
                {
                    log.info("No more messages");
                    return false;
                }
            }
        }

        if (iterator.hasNext())
        {
            MessageAndOffset messageOffset = iterator.next();
            Message message = messageOffset.message();
            key.set(watermark - message.size() - 4);
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            numProcessedMessages++;
            if (!iterator.hasNext())
            {
                messages = null;
                iterator = null;
            }
            return true;
        }
        log.warn("Unexpected iterator end.");
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
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float)(latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException
    {
        if (numProcessedMessages >0)
        {
            log.info("NUM PROCESSED MESSAGED " + numProcessedMessages);

            ZkUtils zk = new ZkUtils(
                conf.get("kafka.zk.connect"),
                conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
                conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
            );

            String group = conf.get("kafka.groupid");
            String partition = split.getBrokerId() + "-" + split.getPartition();
            zk.commitLastConsumedOffset(group, split.getTopic(), partition, watermark);
            zk.close();
        }
        consumer.close();
    }

    private long getEarliestOffset() {
        if (earliestOffset <= 0) {
            earliestOffset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
        }
        return earliestOffset;
    }

    private long getLatestOffset() {
        if (latestOffset <= 0) {
            latestOffset = consumer.getOffsetsBefore(topic, partition, -1L, 1)[0];
        }
        return latestOffset;
    }

    private void resetWatermark(long offset) {
        if (offset <= 0) {
            offset = consumer.getOffsetsBefore(topic, partition, -2L, 1)[0];
            log.info("Resetting offset to smallest {}", offset);
        }
        watermark = earliestOffset = offset;
    }

}
