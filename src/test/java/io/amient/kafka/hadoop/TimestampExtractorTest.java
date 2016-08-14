package io.amient.kafka.hadoop;

import io.amient.kafka.hadoop.api.TimestampExtractor;
import io.amient.kafka.hadoop.io.KafkaInputSplit;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimestampExtractorTest {

    private MapDriver<MsgMetadataWritable, BytesWritable, MsgMetadataWritable, BytesWritable> mapDriver;

    @Before
    public void setUp() {
        mapDriver = MapDriver.newMapDriver(new HadoopJobMapper());
    }

    @Test
    public void testTextTimeExtractor() throws IOException {
        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "texttopic", 3, 1234567890L);
        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getStartOffset());

        String data = "2016-10-01 12:35:00\tSome log data prefixed with timestamp";
        mapDriver.withInput(inputKey, new BytesWritable(data.getBytes()));

        MyTextTimestampExtractor extractor = new MyTextTimestampExtractor();
        HadoopJobMapper.configureTimestampExtractor(mapDriver.getConfiguration(), extractor.getClass().getName());

        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
        MsgMetadataWritable metadata = result.get(0).getFirst();
        assertEquals("2016-10-01 12:35:00", extractor.format(metadata.getTimestamp()));
    }

    public static class MyTextTimestampExtractor implements TimestampExtractor {
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public long extract(MsgMetadataWritable key, BytesWritable value) throws RuntimeException {
            try {
                String leadString = new String(Arrays.copyOfRange(value.getBytes(), 0, 19));
                return parser.parse(leadString).getTime();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }

        public String format(long timestamp) {
            return parser.format(timestamp);
        }
    }


    @Test
    public void testJsonTimeExtractor() throws IOException {
        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "texttopic", 3, 1234567890L);
        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getStartOffset());

        String data = "{\"version\":5,\"timestamp\":1402944501425,\"date\":\"2014-06-16\",\"utc\":1402944501}";
        mapDriver.withInput(inputKey, new BytesWritable(data.getBytes()));

        TimestampExtractor extractor = new MyJsonTimestampExtractor();
        HadoopJobMapper.configureTimestampExtractor(mapDriver.getConfiguration(), extractor.getClass().getName());

        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
        assertEquals(1402944501425L, result.get(0).getFirst().getTimestamp());
        assertEquals(data, new String(result.get(0).getSecond().copyBytes()));
    }

    public static class MyJsonTimestampExtractor implements TimestampExtractor {
        ObjectMapper jsonMapper = new ObjectMapper();

        @Override
        public long extract(MsgMetadataWritable key, BytesWritable value) throws RuntimeException {
            try {
                JsonNode x = jsonMapper.readValue(value.getBytes(), JsonNode.class);
                return x.get("timestamp").getLongValue();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
