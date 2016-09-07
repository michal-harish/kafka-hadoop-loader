package io.amient.kafka.hadoop.api;

import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;

public interface Extractor {

    /**
     * TODO this could be used directly with configured Deserializer kafkaDeserializer;
     * @param key metadata associated with the message
     * @param value message payload byte buffer
     * @return deserialized object that can be processed for extraction of values
     * @throws IOException
     */
    Object deserialize(MsgMetadataWritable key, BytesWritable value) throws IOException;

}
