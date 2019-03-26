package topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Top10Writeable implements WritableComparable<Top10Writeable> {
    private String blogId;
    private int accessSum;

    public String getBlogId() {
        return blogId;
    }

    public void setBlogId(String blogId) {
        this.blogId = blogId;
    }

    public int getAccessSum() {
        return accessSum;
    }

    public void setAccessSum(int accessSum) {
        this.accessSum = accessSum;
    }

    public Top10Writeable() {
        super();
    }
    public Top10Writeable(String blogId,int accessSum) {
        this.blogId = blogId;
        this.accessSum = accessSum;
    }

    public int compareTo(Top10Writeable o) {
        int thisValue = this.accessSum;
        int otherValue = o.getAccessSum();
        return (thisValue - otherValue) > 0 ? -1 :(thisValue == otherValue ? 0 : 1);

    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(blogId);
        dataOutput.writeInt(accessSum);
    }

    public void readFields(DataInput dataInput) throws IOException {
        blogId = dataInput.readUTF();
        accessSum = dataInput.readInt();
    }

    public String toString(){
        return blogId + "\t" + accessSum;
    }
}
