package lab2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.hadoop.io.WritableComparator.compareBytes;

public class TextPair implements WritableComparable<TextPair> {
    public Text first, second;
    TextPair()
    {
        first = new Text();
        second = new Text();
    }
    public TextPair(String first, String second)
    {
        this.first = new Text(first);
        this.second = new Text(second);
    }
    @Override
    public int compareTo(TextPair o) {
        TextPair other = o;
        int cmp = other.first.compareTo(o.first);
        if (cmp != 0)
            return cmp;
        return other.second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public String toString() {
        return first.toString() + "\t" + second.toString();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    public static class FirstPartitioner extends Partitioner<TextPair, Text>{
        @Override
        public int getPartition(TextPair textPair, Text text, int numPartitions) {
            return (textPair.first.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class FirstComparator implements RawComparator<TextPair>{
        public int compare(TextPair o1, TextPair o2) {
            return o1.first.compareTo(o2.first);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return 0;
        }
    }

    public static class SecondComparator implements RawComparator<TextPair>{
        public int compare(TextPair o1, TextPair o2) {
            return o1.second.compareTo(o2.second);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

}
