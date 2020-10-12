package lab2.mappers;

import lab2.KeyPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DictionaryMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().startsWith("Code,Description")){
            return;
        }
        String[] info  = value.toString().replaceAll("\"","").split(",");
        context.write(new KeyPair(info[0], "0"), new Text(info[1]));
    }
}
