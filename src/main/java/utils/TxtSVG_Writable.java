package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TxtSVG_Writable implements Writable{
    private int count = 0;
    private int avg = 0;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getAvg() {
        return avg;
    }

    public void setAvg(int avg) {
        this.avg = avg;
    }

    @Override
    public String toString() {
        return count + "\t" + avg;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeInt(avg);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        avg = dataInput.readInt();
    }
}
