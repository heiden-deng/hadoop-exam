package completesort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CompleteSortReducer extends Reducer<IntWritable,IntWritable,IntWritable,NullWritable>{
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values){
            context.write(value,NullWritable.get());
        }
    }
}
