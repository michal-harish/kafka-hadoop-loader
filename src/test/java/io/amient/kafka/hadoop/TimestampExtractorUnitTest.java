package io.amient.kafka.hadoop;

import io.amient.kafka.hadoop.api.TimestampExtractor;
import io.amient.kafka.hadoop.io.KafkaInputSplit;
import io.amient.kafka.hadoop.io.MsgMetadataWritable;
import io.amient.kafka.hadoop.testutils.MyJsonTimestampExtractor;
import io.amient.kafka.hadoop.testutils.MyTextTimestampExtractor;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimestampExtractorUnitTest {

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


    @Test
    public void testJsonTimeExtractor() throws IOException {
        KafkaInputSplit split = new KafkaInputSplit(1, "host-01", "texttopic", 3, 1234567890L);
        MsgMetadataWritable inputKey = new MsgMetadataWritable(split, split.getStartOffset());

        String data = "{\"version\":5,\"timestamp\":1402944501425,\"date\":\"2014-06-16\",\"utc\":1402944501}";
        mapDriver.withInput(inputKey, new BytesWritable(data.getBytes()));

        TimestampExtractor extractor = new MyJsonTimestampExtractor();
        HadoopJobMapper.configureTimestampExtractor(mapDriver.getConfiguration(), extractor.getClass().getName());

        final List<Pair<MsgMetadataWritable, BytesWritable>> result = mapDriver.run();
        assertEquals(new Long(1402944501425L), result.get(0).getFirst().getTimestamp());
        assertEquals(data, new String(result.get(0).getSecond().copyBytes()));
    }


}
