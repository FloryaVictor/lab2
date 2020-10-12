package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<KeyPair, Text, Text, Text> {
    @Override
    protected void reduce(KeyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        String airport = iter.next()+"";
        if (iter.hasNext()) {
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            float sum = 0.0f;
            int count = 0;
            while (iter.hasNext()) {
                float delay = Float.parseFloat(iter.next().toString());
                min = Float.min(delay, min);
                max = Float.max(delay, max);
                sum += delay;
                count++;
            }
            context.write(new Text(airport), new Text(
                    (sum / count) +
                            "\t" + min +
                            "\t" + max)
            );
        }
    }
}
