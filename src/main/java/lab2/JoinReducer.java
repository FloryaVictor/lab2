package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text code = iter.next();
        if (iter.hasNext()){
            int min = Integer.MAX_VALUE;
            int max = Integer.MIN_VALUE;
            int count = 0, sum = 0;
            while (iter.hasNext())
            {
                int delay = Integer.parseInt(iter.next().toString());
                min = Integer.min(delay, min);
                max = Integer.max(delay, max);
                sum += delay;
                count ++;
            }
            context.write(code, new Text(
                    Integer.toString(sum / count) +
                    " " + Integer.toString(min) +
                    " " + Integer.toString(max))
            );
        }
    }
}
