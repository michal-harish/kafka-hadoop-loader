kafka-hadoop-consumer - OVERVIEW
================================

This hadoop consumer creates splits for each broker-topic-partition which creates
ideal parallelism between kafka sterams and mapper tasks.

Further it does not use high level consumer and communicates with zookeeper directly
for management of the consumed offsets, which are comitted at the end of each map task,
that is when the output file has been moved from hdfs_temp to its final destination. 

The actual consumer and it's inner fetcher thread are wrapped as KafkaInputContext which
is created for each Map Task's record reader object.

The mapper then takes in offest,message pairs, parses the content for date and emits (date,message)
which is in turn picked up by Output Format and partitioned on the hdfs-level to different location.


ANATOMY
-------

    HadoopJob
        -> KafkaInputFormat
            -> zkUtils.getBrokerPartitions 
            -> FOR EACH ( broker-topic-partition ) CREATE KafkaInputSplit
        -> FOR EACH ( KafkaInputSplit ) CREATE MapTask:
            -> KafkaInputRecordReader( KafkaInputSplit[i] )
                -> zkUtils.getLastConsumedOffset
                -> KafkaInputContext 
                    -> ArrayBlockingQueue
                    -> KafkaInputContext.FetchThread ( ! the darkest corner of the hadoop loader )
                        -> SimpleConsumer
                        -> start() -> run() { FetchRequest -> add messages to FetchRequest.queue -> ..}
                -> WHILE ( KafkaInputContext.hasMore() )
                    -> KafkaInputContext.FetchThread.fetchMore() 
                        -> poll FetchRequest.queue
                -> READ nextKeyValue()
                    -> KafkaInputContext.getNext() -> (offset,message):newOffset
                    -> KafkaInputRecordReader advance currentOffset+=newOffset and numProcessedMessages++
                    -> HadoopJobMapper(offset,message) -> (date, message)
                        -> KafkaOutputFormat.RecordWriter.write(date, message)
                            -> recordWriters[date].write( date,message )
                                -> LineRecordWriter.write( message ) gz compressed or not
                -> END WHILE
                -> close KafkaInputContext
                -> zkUtils.commitLastConsumedOffset


LAUNCH CONFIGURATIONS
=====================

ARGUMENTS FOR RUNNING ON STAG KAFKA CENTRAL CLUSTER (CONTINUOUIS )
------------------------------------------------------------------
hadoop jar /usr/share/kafka/kafka-hadoop-loader.jar -t adviews,adclicks,pageviews,conversions,datasync,useractivity -z zookeeper-01.stag.visualdna.com:2181,zookeeper-02.stag.visualdna.com:2181,zookeeper-03.stag.visualdna.com:2181 -o 0 -l 5000000 -i json -r namenode-01.stag.visualdna.com /vdna/events-streamed

ARGUMENTS FOR RUNNING IN SIMULATION MODE FROM DEV CLUSTER (RESTART)
-------------------------------------------------------------------
-r 10.100.8.132 -t sim_tracking_events -z hq-mharis-d01:2181 -o 0 -l 5000000 -i binary /tmp/events


TO RUN FROM ECLIPSE (NO JAR)
----------------------------
    add run configuration arguments: -r [-t <coma_separated_topic_list>] [-z <zookeeper>] [target_hdfs_path]


TO RUN REMOTELY
---------------
    $ mvn assembly:single
    $ java -jar kafka-hadoop-loader.jar -r [-t <coma_separated_topic_list>] [-z <zookeeper>] [target_hdfs_path]
    TODO -r check if jar exists otherwise use addJarByClass


TO RUN AS HADOOP JAR
--------------------
    $ mvn assembly:single
    $ hadoop jar kafka-hadoop-loader.jar [-z <zookeeper>] [-t <topic>] [target_hdfs_path]





