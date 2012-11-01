package net.imagini.kafka.hadoop;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtils implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);

    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String    BROKER_TOPICS_PATH = "/brokers/topics";

    private ZkClient client ;
    Map<String, String> brokers ;

    public ZkUtils(Configuration config) {
        connect(config);
    }

    private void connect(Configuration config) {
        LOG.info("Connecting zk");
        String zk = config.get("kafka.zk.connect");
        int stimeout = config.getInt("kafka.zk.sessiontimeout.ms", 10000);
        int ctimeout = config.getInt("kafka.zk.connectiontimeout.ms", 10000);
        client = new ZkClient(zk, stimeout, ctimeout, new StringSerializer() );
        LOG.info("Connected zk");
    }

    public String getBrokerName(String id) {
        if (brokers == null) {
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
            for(String bid: brokerIds) {
                String data = client.readData(BROKER_IDS_PATH + "/" + bid);
                LOG.info("Broker " + bid + " " + data);
                brokers.put(bid, data.split(":", 2)[1]);
            }
        }
        return brokers.get(id);
    }

    public List<String> getBrokerPartitions(String topic) {
        List<String> partitions = new ArrayList<String>();
        List<String> brokersTopics = getChildrenParentMayNotExist( BROKER_TOPICS_PATH + "/" + topic);
        for(String broker: brokersTopics) {
            String parts = client.readData(BROKER_TOPICS_PATH + "/" + topic + "/" + broker);
            for(int i =0; i< Integer.valueOf(parts); i++) {
                partitions.add(broker + "-" + i);
            }
        }
        return partitions;
    }

    private String getOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }

    public long getLastConsumedOffset(String group, String topic, String partition) {
        String znode = getOffsetsPath(group ,topic ,partition);
        String offset = client.readData(znode, true);
        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }

    public void commitLastConsumedOffset(
        String group, 
        String topic, 
        String partition, 
        long offset
    )
    {
        String path = getOffsetsPath(group ,topic ,partition);

        LOG.info("OFFSET COMMIT " + path + " = " + offset);
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
        //TODO JIRA:EDA-4 use versioned zk.writeData in case antoher hadooop loaer has advanced the offset
        client.writeData(path, offset);
    }


    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = client.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }

    public void close() throws java.io.IOException {
        if (client != null) {
            client.close();
        }
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {}
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
    }
}
