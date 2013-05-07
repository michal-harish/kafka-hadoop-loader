package co.gridport.kafka.hadoop;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class FetcherTest {
    public static void main(String[] args) throws Exception {

        final List<String> seeds = Arrays.asList("localhost");
        final String topic = "test";
        final List<Integer> partitions = discoverPartitions(topic, seeds);
        ExecutorService executor = Executors.newFixedThreadPool(partitions.size());
        for(final Integer partition: partitions) {
            executor.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        KafkaInputFetcher fetcher = new KafkaInputFetcher(
                            "HadoopLoaderFetcher",
                            topic,
                            partition, 
                            seeds,
                            30000,
                            64 * 1024
                        );
                        while(true) {
                            MessageAndOffset messageAndOffset;
                            try {
                                messageAndOffset = fetcher.nextMessageAndOffset(65535 * 16);
                                if (messageAndOffset == null) {
                                    //backoff sleep
                                    try {Thread.sleep(1000);} catch (InterruptedException ie) {}
                                } else {
                                    ByteBuffer payload = messageAndOffset.message().payload();
                                    byte[] bytes = new byte[payload.limit()];
                                    payload.get(bytes);
                                    System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                                }
                            } catch (Exception e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                }
            );
        }

    }

    private static List<Integer> discoverPartitions(String topic,List<String> seeds) {
        List<Integer> result = new LinkedList<Integer>();
        for(String seed: seeds) {
            SimpleConsumer consumer = new SimpleConsumer(seed, 9092, 10000, 65535, "PartitionsLookup");
            TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(topic));
            TopicMetadataResponse response = consumer.send(request);
            if (response != null && response.topicsMetadata() != null) {
                for(TopicMetadata tm: response.topicsMetadata()) {
                    for(PartitionMetadata partMd: tm.partitionsMetadata()) {
                        result.add(partMd.partitionId());
                    }
                }
            }
        }
        return result;
    }

}
