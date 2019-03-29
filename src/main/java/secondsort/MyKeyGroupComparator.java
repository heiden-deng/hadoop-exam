package secondsort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyKeyGroupComparator extends WritableComparator{
    public MyKeyGroupComparator() {
        super(IntPair.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair ip1 = (IntPair)a;
        IntPair ip2 = (IntPair)b;
        if (ip1.getA() == ip2.getA()){
            return 0;
        }
        else{
            return ip1.getA() > ip2.getA() ? 1 : -1;
        }
    }
}
