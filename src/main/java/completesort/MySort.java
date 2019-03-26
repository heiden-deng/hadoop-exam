package completesort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator{
    public MySort(){
        super(IntWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable v1 = (IntWritable)a;
        IntWritable v2 = (IntWritable)b;
        return v2.compareTo(v1);
    }
}
