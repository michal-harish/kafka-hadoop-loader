package net.imagini.kafka.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import co.gridport.protos.VDNAEvent.Event;

import com.googlecode.protobuf.format.JsonFormat;

public class HadoopJobMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {
    @Override
    public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
        Text date = new Text();
        Text out = new Text();
        try {
            if (context.getConfiguration().get("input.format").equals("json"))
            {
                //grab the raw json date
                String json = new String(value.getBytes());
                int i = json.indexOf(",\"date\":\"") + 9;
                date.set(value.getBytes(), i, 10);
                out.set(value.getBytes(),0, value.getLength());
            } else if (context.getConfiguration().get("input.format").equals("binary"))
            {
                //Open the proto
                Event event = Event.parseFrom(value.copyBytes());
                date.set(event.getDate());
                out.set( JsonFormat.printToString(event));
            }
            else
            {
                throw new IOException("Invalid mapper input.format");
            }
            context.write(date, out);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
