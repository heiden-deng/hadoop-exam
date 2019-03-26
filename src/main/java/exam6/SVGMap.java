package exam6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SVGMap extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), new IntWritable(Integer.parseInt(value.toString())));
    }
}
