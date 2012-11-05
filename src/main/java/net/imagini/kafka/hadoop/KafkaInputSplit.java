package net.imagini.kafka.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class KafkaInputSplit extends InputSplit implements Writable {

    private String brokerId;
    private String broker;
    private int partition;
    private String topic;
    private long lastCommit;

    public KafkaInputSplit() {}

    public KafkaInputSplit(String brokerId, String broker, String topic, int partition, long lastCommit) {
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

    /**
     * @return broker-id:host:port
     */
    public String getBroker() {
        return broker;
    }

    public String getBrokerHost()
    {
        String[] hostPort = broker.split(":");
        return hostPort[0];
    }

    public int getBrokerPort()
    {
        String[] hostPort = broker.split(":");
        return Integer.valueOf(hostPort[1]);
    }
    
    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getWatermark() {
        return lastCommit;
    }

    @Override
    public String toString() {
        return broker + "-" + topic + "-" + partition + "-" + lastCommit ;
    }
}