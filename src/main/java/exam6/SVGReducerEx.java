package exam6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import utils.TxtSVG_Writable;

import java.io.IOException;

public class SVGReducerEx extends Reducer<IntWritable, TxtSVG_Writable,IntWritable,TxtSVG_Writable> {
    private TxtSVG_Writable w = new TxtSVG_Writable();

    @Override
    protected void reduce(IntWritable key, Iterable<TxtSVG_Writable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (TxtSVG_Writable val : values){
            sum += val.getCount() * val.getAvg();
            count += val.getCount();
        }
        w.setCount(count);
        w.setAvg(sum/count);
        context.write(key, w);
    }
}
