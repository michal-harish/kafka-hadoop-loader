kafka-hadoop-loader
=====================

This hadoop loader creates splits for each topic-broker-partition which creates
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
                -> intialize simple kafka consumer
                -> reset watermark if given as option
                -> WHILE nextKeyValue()
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





