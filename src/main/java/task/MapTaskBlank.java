package task;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapTaskBlank extends Mapper<Object,Text,Text,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] str = value.toString().split(" ");
        context.write(new Text(str[0]), new Text("mapA," + value.toString()));
    }
}
