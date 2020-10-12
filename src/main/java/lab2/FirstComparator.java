package lab2;

import org.apache.hadoop.io.RawComparator;

public class FirstComparator implements RawComparator<TextPair> {
    public int compare(TextPair o1, TextPair o2) {
        return o1.first.compareTo(o2.first);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}