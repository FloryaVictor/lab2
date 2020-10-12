package lab2;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//public class FirstComparator implements RawComparator<KeyPair> {
//    public int compare(KeyPair o1, KeyPair o2) {
//        return o1.id.compareTo(o2.id);
//    }
//
//    @Override
//    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//        return 0;
//    }
//}

public class FirstComparator extends WritableComparator{
    protected FirstComparator(){
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((KeyPair)a).id.get() - ((KeyPair)b).id.get();
    }
}