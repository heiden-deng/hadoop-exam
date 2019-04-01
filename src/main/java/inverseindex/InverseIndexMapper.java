package inverseindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InverseIndexMapper extends Mapper<LongWritable,Text,Text,Text>{

    private Text k = new Text();
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        org.apache.hadoop.mapreduce.lib.input.FileSplit fileSplit = (org.apache.hadoop.mapreduce.lib.input.FileSplit)context.getInputSplit();
        String path = fileSplit.getPath().toString();
        for (int i =0 ;i < words.length; i++){
            k.set(words[i] + "->" + path);
            v.set("1");
            context.write(k,v);
        }
    }
}
