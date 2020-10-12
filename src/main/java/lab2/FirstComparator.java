package lab2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FirstComparator extends WritableComparator{
    protected FirstComparator(){
        super(KeyPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((KeyPair)a).id.get() - ((KeyPair)b).id.get();
    }
}