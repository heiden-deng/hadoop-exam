package task;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapTaskComma extends Mapper<Object,Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        context.write(new Text(str[0]), new Text("mapB," + value.toString()));
    }
}
