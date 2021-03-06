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

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0){
            return;
        }
        String[] raw = value.toString().replaceAll("\"","").split(",");
        if (!raw[IS_CANCELLED].equals(ONE) && !raw[DELAY].equals("")) {
            float delay = Float.parseFloat(raw[DELAY]);
            if (delay > 0) {
                int id = Integer.parseInt(raw[ID]);
                context.write(new KeyPair(id, 1), new Text(raw[DELAY]));
            }
        }
    }
}
