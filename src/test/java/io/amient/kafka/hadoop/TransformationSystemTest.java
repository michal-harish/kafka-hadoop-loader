package io.amient.kafka.hadoop;

import io.amient.kafka.hadoop.api.TimestampExtractor;
import io.amient.kafka.hadoop.api.Transformation;
import io.amient.kafka.hadoop.io.KafkaInputFormat;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import io.amient.kafka.hadoop.io.MultiOutputFormat;
import io.amient.kafka.hadoop.testutils.SystemTestBase;
import kafka.producer.KeyedMessage;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransformationSystemTest extends SystemTestBase {


    static public class TSVToJson implements Transformation, TimestampExtractor {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public Object deserialize(MsgMetadataWritable key, BytesWritable value) throws IOException {
            String[] tsv = new String(value.getBytes(), 0, value.getLength(), "UTF-8").split("\t");
            ObjectNode json = mapper.createObjectNode();
            json.put("category", tsv[0]);
            json.put("quantity", Double.parseDouble(tsv[1]));
            json.put("timestamp", Long.parseLong(tsv[2]));
            return json;
        }

        @Override
        public Long extractTimestamp(Object any) throws IOException {
            return ((JsonNode) any).get("timestamp").getLongValue();
        }

        @Override
        public BytesWritable transform(Object any) throws IOException {
            JsonNode json = (JsonNode) any;
            return new BytesWritable(mapper.writeValueAsBytes(json));
        }

    }

    /**
     * this test uses a custom schema transformation. the tsv file has
     * a `known` field mapping provided by the transformation implementation.
     *
     * @throws Exception
     */
    @Test
    public void canTransformTSVInputToJSON() throws Exception {
        //produce text data
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "A\t1.0\t1473246194481"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key2", "B\t1.5\t1473246214844"));
        simpleProducer.send(new KeyedMessage<>("topic01", "key1", "C\t2.0\t1473246220528"));

        //configure inputs, timestamp extractor and the output path format
        KafkaInputFormat.configureKafkaTopics(conf, "topic01");
        KafkaInputFormat.configureZkConnection(conf, zkConnect);
        HadoopJobMapper.configureExtractor(conf, TSVToJson.class);
        MultiOutputFormat.configurePathFormat(conf, "'{T}'");

        //run the first job
        Path outDir = runSimpleJob("topic01", "canTransformTSVInputToJSON");

        Path output1 = new Path(outDir, "topic01/topic01-0-0000000000000000000");
        assertTrue(localFileSystem.exists(output1));
        String json1 = readFullyAsString(output1, 1000);
        assertEquals("{\"category\":\"A\",\"quantity\":1.0,\"timestamp\":1473246194481}\n" +
                "{\"category\":\"C\",\"quantity\":2.0,\"timestamp\":1473246220528}\n", json1);

        Path output2 = new Path(outDir, "topic01/topic01-1-0000000000000000000");
        assertTrue(localFileSystem.exists(output2));
        String json2 = readFullyAsString(output2, 1000);
        assertEquals("{\"category\":\"B\",\"quantity\":1.5,\"timestamp\":1473246214844}\n", json2);

    }

//    /**
//     * this test uses a custom schema transformation. the tsv file has
//     * a pre-defined field mapping provided by the
//     * @throws Exception
//     */
//    @Test
//    public void canTransfromJsonInputToParquet() throws Exception {
//        //TODO
//    }

}
