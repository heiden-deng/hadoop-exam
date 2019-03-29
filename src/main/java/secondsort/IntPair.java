package secondsort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair>{
    private int a;
    private int b;

    public IntPair() {
        a = 0;
        b = 0;
    }

    public IntPair(int a, int b) {
        this.a = a;
        this.b = b;
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }

    public int compareTo(IntPair o) {
        if (this.a == o.a){
            if(this.b == o.b){
                return 0;
            }
            else{
                return this.b > o.b ? 1 : -1;
            }
        }else{
            return this.a > o.a ? 1 : -1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(a);
        dataOutput.writeInt(b);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.a = dataInput.readInt();
        this.b = dataInput.readInt();
    }
}
