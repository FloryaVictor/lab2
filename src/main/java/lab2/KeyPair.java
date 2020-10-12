package lab2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyPair implements WritableComparable<KeyPair> {
    public IntWritable id, type;
    KeyPair()
    {
        id = new IntWritable();
        type = new IntWritable();
    }
    public KeyPair(Integer id, Integer type)
    {
        this.id = new IntWritable(id);
        this.type = new IntWritable(type);
    }
    @Override
    public int compareTo(KeyPair o) {
        int cmp = id.compareTo(o.id);
        if (cmp != 0)
            return cmp;
        return type.compareTo(o.type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        type.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        type.readFields(in);
    }

    @Override
    public String toString() {
        return id.toString() + "\t" + type.toString();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

}
