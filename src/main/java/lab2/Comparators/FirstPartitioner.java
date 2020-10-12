package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<KeyPair, Text> {
    @Override
    public int getPartition(KeyPair keyPair, Text text, int numPartitions) {
        return keyPair.id.get() % numPartitions;
    }
}