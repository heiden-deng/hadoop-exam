package exam6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.TxtSVG_Writable;

import java.io.IOException;

public class SVGMapEx extends Mapper<LongWritable,Text,IntWritable,TxtSVG_Writable> {
    private TxtSVG_Writable  w = new TxtSVG_Writable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        w.setCount(1);
        w.setAvg(Integer.parseInt(value.toString()));
        context.write(new IntWritable(1), w);
    }
}
