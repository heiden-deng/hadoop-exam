package secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondMapper extends Mapper<LongWritable,Text,IntPair,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer strTok = new StringTokenizer(value.toString());
        int a = Integer.parseInt(strTok.nextToken());
        int b = Integer.parseInt(strTok.nextToken());
        context.write(new IntPair(a,b),new IntWritable(b));
    }
}
