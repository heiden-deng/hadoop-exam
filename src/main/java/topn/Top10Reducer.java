package topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.TreeMap;

public class Top10Reducer extends Reducer<NullWritable,Top10Writeable,NullWritable,Text>{
    private TreeMap<Top10Writeable,Text> visittimeMap = new TreeMap<Top10Writeable, Text>();
    @Override
    protected void reduce(NullWritable key, Iterable<Top10Writeable> values, Context context) throws IOException, InterruptedException {
        for (Top10Writeable value : values){
            //String[] strs = value.toString().split(" ");
            visittimeMap.put(value,new Text(value.getBlogId()));
            if (visittimeMap.size() > 10){
                visittimeMap.remove(visittimeMap.firstKey());
            }
        }
        for (Top10Writeable t: visittimeMap.keySet()){
            context.write(NullWritable.get(),new Text(t.toString()));
        }
    }
}
