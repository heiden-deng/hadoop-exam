package join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BJoinMapper extends Mapper<Object,Text,Text,Text>{
    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(" ");
        String joindata = strs[0];
        if (joindata == null){
            return;
        }
        outkey.set(joindata);
        outvalue.set("B"+value.toString());
        context.write(outkey,outvalue);
    }
}
