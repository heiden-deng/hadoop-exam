package topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class Top10Mapper extends Mapper<Object,Text,NullWritable,Top10Writeable>{
    private TreeMap<Top10Writeable,Text> visittimeMap = new TreeMap<Top10Writeable, Text>();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null){
            return ;
        }
        String[] strs = value.toString().split(" ");
        String tId = strs[0];
        String reputation = strs[1];
        if (tId == null || reputation == null){
            return;
        }
        Top10Writeable res = new Top10Writeable(tId,Integer.parseInt(reputation));
        visittimeMap.put(res,new Text(value));
        if (visittimeMap.size() > 10){
            visittimeMap.remove(visittimeMap.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Top10Writeable t : visittimeMap.keySet()){
            context.write(NullWritable.get(),t);
        }
    }
}
