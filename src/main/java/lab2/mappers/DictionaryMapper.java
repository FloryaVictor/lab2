package lab2.mappers;

import lab2.KeyPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DictionaryMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    private static final int ID = 0;
    private static final int NAME = 0;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0){
            return;
        }
        String[] raw  = value.toString().replaceAll("\"","").split(",");
        context.write(new KeyPair(Integer.parseInt(raw[ID]), 0), new Text(raw[NAME]));
    }
}
