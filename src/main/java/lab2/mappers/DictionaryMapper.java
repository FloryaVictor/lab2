package lab2.mappers;

import lab2.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DictionaryMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] info  = value.toString().split(",");
        context.write(new TextPair(info[0].replace("\"",""), "0"), new Text(info[1]));
    }
}
