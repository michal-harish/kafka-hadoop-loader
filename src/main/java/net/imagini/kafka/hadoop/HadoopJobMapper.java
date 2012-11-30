package net.imagini.kafka.hadoop;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HadoopJobMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {

    @Override
    public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
        try {
            Text outValue = new Text();
            Text outDateKey = map(key, value, context.getConfiguration());
            if (outDateKey != null) {
                context.write(outDateKey, outValue);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Text map(LongWritable key, BytesWritable value, Configuration conf) throws IOException
    {
        String inputFormat = conf.get("input.format");
        if (inputFormat.equals("json"))
        {
            //grab the raw json date
            String json = null;
            try {
                json = new String(value.getBytes());
                int i = json.indexOf(",\"date\":\"") + 9;
                Text outDateKey = new Text();
                outDateKey.set(value.getBytes(), i, 10);
                return outDateKey;
            } catch (Exception e) {
                //JIRA EDA-23
                System.err.println("Failed to parse json event message `" + json + "`");
                return null;
            }
        } else if (inputFormat.equals("binary"))
        {
            //Open the proto
            throw new NotImplementedException("Protos disabled");
            /*
            Event event = Event.parseFrom(value.copyBytes());
            date.set(event.getDate());
            out.set( JsonFormat.printToString(event));
            */
        }
        else
        {
            throw new IOException("Invalid mapper input.format");
        }

    }
}
