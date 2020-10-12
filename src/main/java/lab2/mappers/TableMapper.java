package lab2.mappers;

import lab2.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info  = value.toString().split(",");
        try {
            if (Float.parseFloat(info[18]) > 0)
                context.write(new TextPair(info[14], "1"), new Text(info[18]));
        }catch (Exception e) {
            if (e.getClass() == IOException.class || e.getClass() == InterruptedException.class){
                throw e;
            }
        }
    }
}
