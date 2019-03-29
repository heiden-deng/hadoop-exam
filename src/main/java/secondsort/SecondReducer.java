package secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondReducer extends Reducer<IntPair,IntWritable,IntWritable,IntWritable>{
    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable myKey = new IntWritable(key.getA());
        for (IntWritable i : values){
            context.write(myKey,i);
        }
    }
}
