package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
    @Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println(key.first);
        Iterator<Text> iter = values.iterator();
        Text airport = iter.next();
        if (iter.hasNext()){
            float min = Float.MAX_VALUE;
            float max = Float.MIN_VALUE;
            float sum = 0.0f;
            int count = 0;
            while (iter.hasNext()) {
                try {
                    float delay = Float.parseFloat(iter.next().toString());
                    min = Float.min(delay, min);
                    max = Float.max(delay, max);
                    sum += delay;
                    count++;
                    context.write(airport, new Text(
                            Float.toString(sum / count) +
                                    " " + Float.toString(min) +
                                    " " + Float.toString(max))
                    );
                }catch (Exception e){
                    if (e.getClass() == IOException.class || e.getClass() == InterruptedException.class){
                        throw e;
                    }
                }
            }
        }
    }
}
