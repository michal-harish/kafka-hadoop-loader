# kafka-hadoop-loader

This is a hadoop job for incremental loading of kafka topics into hdfs.
It works by creating a split for each partition, whether physical (
pre kafka v.0.8) or logical( kafka v0.8 or higher). It differs from
the most comprehensive solution (Kafka Connect) mainly in that it works
for simple, schema-less cases directly off kafka brokers without
 having to integrate with any other components.

# Offset-tracking

It uses hdfs for check-pointing by defauit (a bit more work needs to be done
to guarantee exactly-once delivery) but offers also a switch to use kafka's
consumer group mechanism via zookeeper store. Each split is assigned 
to a task and when that task completes loading all the up-to-date
messages in the given partition, the output file for the task is 
 committed using the standard hadoop output committer mechanism.


# Output Partitioning

The output paths are configurable via formatter which uses placeholders
for topic name `{T}` and partition id `{P}` and the default format is:

    '{T}/{P}'
    
This default format basically follows kafka partitioning model which
translates the output file paths as follows:

    <job-output-directory>/<topic-name>/<partition-id>/<unique-filename>

job-output-directory is fixed and unique-filename is a combination of
topic partition and start offset where the incremental load started. 

Optionally, a TimestampExtractor may be provided by configuration
which enables time based partitioning format, for example:

    '{T}/d='dddd-MM-yy 

would result in the following file paths:

    <job-output-directory>/<topic-name>/d=<date>/<unique-filename>
    
From Kafka 0.10 each message has a default timestamp metadata which
will be available automatically on the 0.10 and higher versions of 
the hadoop loader.

# Schema-less model

It is schema-less and when used with the built-in MutliOutputFormat it
simply writes out each message payload byte-by-byte on a new line. This
way it can be used for simple csv/tsv/json encodings.

Hadoop Loader is capable of transformations within the mapper, be it
schema-based or purpose-formatted but for these use cases, 
Kafka Connect is much more suitable framework.


# OUT-OF-THE-BOX LAUNCH CONFIGURATIONS

The default program can be packaged with `mvn package` which produces
a jar that has a limited functionality via command line arguments.
To see how the job can be configured and extended programatically
see system tests under src/test.

## TO RUN FROM AN IDE
    add run configuration arguments: -r [-t <coma_separated_topic_list>] [-z <zookeeper>] [target_hdfs_path]


## TO RUN REMOTELY

    $ mvn package
    $ java -jar kafka-hadoop-loader.jar -r [-t <coma_separated_topic_list>] [-z <zookeeper>] [target_hdfs_path]
    TODO -r check if jar exists otherwise use addJarByClass


## TO RUN AS HADOOP JAR
    $ mvn package
    $ hadoop jar kafka-hadoop-loader.jar [-z <zookeeper>] [-t <topic>] [target_hdfs_path]



# ANATOMY

    HadoopJob
        -> KafkaInputFormat
            -> zkUtils.getBrokerPartitionLeaders
            -> FOR EACH ( logical partition ) CREATE KafkaInputSplit
        -> FOR EACH ( KafkaInputSplit ) CREATE MapTask:
            -> KafkaInputRecordReader( KafkaInputSplit[i] )
                -> checkpoint manager getLastConsumedOffset
                -> intialize simple kafka consumer
                -> reset watermark if given as option
                -> WHILE nextKeyValue()
                    -> KafkaInputContext.getNext() -> (offset,message):newOffset
                    -> KafkaInputRecordReader advance currentOffset+=newOffset and numProcessedMessages++
                    -> HadoopJobMapper(offset,message) -> (date, message)
                        -> KafkaOutputFormat.RecordWriter.write(date, message)
                            -> recordWriters[date].write( date,message )
                                -> LineRecordWriter.write( message ) gz compressed or not
                -> close KafkaInputContext
                -> zkUtils.commitLastConsumedOffset



