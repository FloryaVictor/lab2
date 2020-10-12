package lab2;

import org.apache.hadoop.io.RawComparator;

public class FirstComparator implements RawComparator<KeyPair> {
    public int compare(KeyPair o1, KeyPair o2) {
        return o1.id.compareTo(o2.id);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return 0;
    }
}