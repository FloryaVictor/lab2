package lab2.Comparators;

import lab2.KeyPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstComparator extends WritableComparator{
    FirstComparator(){
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((KeyPair)a).id.get() - ((KeyPair)b).id.get();
    }
}