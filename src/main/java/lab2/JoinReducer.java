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
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            float sum = 0.0f;
            int count = 0;
            while (iter.hasNext())
            {
                float delay = Float.parseFloat(iter.next().toString());
                min = Float.min(delay, min);
                max = Float.max(delay, max);
                sum += delay;
                count ++;
            }
            context.write(code, new Text(
                    Float.toString(sum / count) +
                    " " + Float.toString(min) +
                    " " + Float.toString(max))
            );
        }
    }
}
