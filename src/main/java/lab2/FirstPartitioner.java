package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<TextPair, Text> {
    @Override
    public int getPartition(TextPair textPair, Text text, int numPartitions) {
        return (textPair.first.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}