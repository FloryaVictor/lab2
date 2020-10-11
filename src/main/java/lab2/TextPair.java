package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    public Text first, second;
    TextPair()
    {
        first = new Text();
        second = new Text();
    }

    @Override
    public int compareTo(TextPair o) {
        TextPair other = (TextPair)o;
        return other.first == this.first && ;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
