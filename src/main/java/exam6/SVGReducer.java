package exam6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SVGReducer  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable val : values){
            sum += val.get();
            count++;
        }
        context.write(new IntWritable(1), new IntWritable(sum/count));
    }
}
