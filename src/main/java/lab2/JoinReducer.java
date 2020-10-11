package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        String code = iter.next().toString();
        if (iter.hasNext()){
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            int count = 0, sum = 0;
            while (iter.hasNext())
            {
                
            }
        }
    }
}
