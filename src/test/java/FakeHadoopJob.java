

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import net.imagini.kafka.hadoop.KafkaInputFormat;
import net.imagini.kafka.hadoop.KafkaInputRecordReader;
import net.imagini.kafka.hadoop.KafkaInputSplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

public class FakeHadoopJob {

    static protected long grandTotal = 0;
    
    static protected Object lock = new Object();

    public static void main(String[] args) throws InterruptedException, IOException
    {
        //Mock the hadoop job with executor of MockMapTask pool
        ExecutorService pool =  Executors.newCachedThreadPool();

        JobConf conf = new JobConf(new Configuration());
        conf.set("kafka.zk.connect", "zookeeper-01.stag.visualdna.com:2181,zookeeper-02.stag.visualdna.com:2181,zookeeper-03.stag.visualdna.com:2181");
        conf.set("kafka.zk.sessiontimeout.ms", "10000");
        conf.set("kafka.zk.connectiontimeout.ms", "10000");
        conf.set("kafka.topics", "adviews,adclicks,pageviews,conversions,datasync,useractivity");
        conf.set("kafka.groupid", "hadoop-loader-test");
        conf.set("kafka.watermark.reset", "earliest");
        
        JobContext dummyJobContext  = new Job(conf);
        
        KafkaInputFormat realInputFormat = new KafkaInputFormat();
        List<InputSplit> splits = realInputFormat.getSplits(dummyJobContext);

        Collection<Callable<String>> tasks = new ArrayList<Callable<String>>();
        for(InputSplit split: splits)
        {
            tasks.add(new MockMapTask(split, conf));
        }

        for(Future<String> future: pool.invokeAll(tasks))
        {
            try {
                System.out.println(future.get());
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.println();
        System.out.println("GRAND TOTAL = " + grandTotal);

        pool.shutdown();

    }

    public static class MockMapTask implements Callable<String>
    {
        private KafkaInputSplit split;
        private KafkaInputRecordReader realRecordReader;
        private String info;

        public MockMapTask(InputSplit split, Configuration conf) throws IOException, InterruptedException
        {
            this.split = (KafkaInputSplit) split;
            info = this.split.getTopic() + "/"  + this.split.getBrokerHost() +":" + this.split.getPartition();
            realRecordReader = new KafkaInputRecordReader();
            realRecordReader.initialize(split, conf);
        }

        public String call() {
            Boolean success = false;
            int count = 0;
            try {
                while(realRecordReader.nextKeyValue())
                {
                    //realRecordReader.getProgress
                    realRecordReader.getCurrentKey();
                    realRecordReader.getCurrentValue();
                    count++;
                }
                realRecordReader.close();
                success = true;
            } catch (Exception e) {
                e.printStackTrace();
                success =false;
            }
            grandTotal += count;
            return (success ? "SUCCESS" : "FAILURE" ) + " Processed "
                + info
                + " count " + count;
        }
    }
}
