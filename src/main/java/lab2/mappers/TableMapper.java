package lab2.mappers;

import lab2.KeyPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, KeyPair, Text> {
    private static final int ID = 14;
    private static final int DELAY = 18;
    private static final int  IS_CANCELLED = 19;

    private static final String ONE = "1.00";
    private static final String ZERO = "0.00";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0){
            return;
        }
        String[] raw = value.toString().replaceAll("\"","").split(",");
        if (raw[])
            if (Float.parseFloat(info[18]) > 0) {
                context.write(new KeyPair(Integer.parseInt(info[14]), 1), new Text(info[18]));
            }
    }
}
