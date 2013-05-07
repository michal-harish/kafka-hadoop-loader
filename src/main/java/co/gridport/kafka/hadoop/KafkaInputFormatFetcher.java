package co.gridport.kafka.hadoop;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;



public class KafkaInputFormatFetcher {


    public static void main(String[] args) throws Exception {

        KafkaInputFormatFetcher fetcher = new KafkaInputFormatFetcher(Arrays.asList("localhost"));

        while(true) {
            MessageAndOffset messageAndOffset = fetcher.nextMessageAndOffset();
            if (messageAndOffset == null) {
                //backoff sleep
                try {Thread.sleep(1000);} catch (InterruptedException ie) {}
            } else {
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
            }
        }
    }

    private List<String> seeds;

    private String clientId = "testClient";
    private String topic = "test";
    private Integer partition = 0;
    private Integer fetchSize = 65535 * 16;

    private String leader;
    private List<String> replicaBrokers = new ArrayList<String>();
    private SimpleConsumer consumer = null;
    private Long offset;
    private ConcurrentLinkedQueue<MessageAndOffset> messageQueue;

    public KafkaInputFormatFetcher(List<String> seeds) {
        this.seeds = seeds;
        messageQueue =  new ConcurrentLinkedQueue<MessageAndOffset>();
    }

    private MessageAndOffset nextMessageAndOffset() throws Exception {

        if (leader == null) {
            leader = findLeader(seeds);
        }
        if (consumer == null) {
            consumer = new SimpleConsumer(leader, 9092, 10000, 65535, clientId);
            offset = getOffset(kafka.api.OffsetRequest.LatestTime());
        }
        if (messageQueue.size() < 100) {
            FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
            FetchRequest req = requestBuilder
                .clientId(clientId)
                .addFetch(topic, partition, offset, fetchSize)
                .build();
            FetchResponse response = consumer.fetch(req);
            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leader + " Reason: " + code);
                consumer.close();
                consumer = null;
                leader = findNewLeader(leader);
            }
            for (MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }
                messageQueue.offer(messageAndOffset);
            }
        }
        if (messageQueue.size() > 0) {
            MessageAndOffset messageAndOffset = messageQueue.poll();
            offset = messageAndOffset.nextOffset();
            return messageAndOffset;
        } else {
            return null;
        }

    }

    private Long getOffset(Long time) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
        OffsetResponse response1 = consumer.getOffsetsBefore(request);

        if (response1.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response1.errorCode(topic, partition) );
            System.exit(1);
        }
        return response1.offsets(topic, partition)[0];
    }

    private String findNewLeader(String oldLeader) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            String newLeader= findLeader(replicaBrokers);
            if (newLeader == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(newLeader) && i == 0) {
                goToSleep = true;
            } else {
                return newLeader;
            }
            if (goToSleep) {
                try { Thread.sleep(1000); } catch (InterruptedException ie) {}
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private String findLeader(List<String> seeds) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seeds) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, 9092, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null && returnMetaData.leader() != null) {
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
            return returnMetaData.leader().host();
        } else {
            return null;
        }
    }
}
