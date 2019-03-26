package completesort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<IntWritable, IntWritable>{
    @Override
    public int getPartition(IntWritable intWritable, IntWritable intWritable2, int i) {
        int keyInt = Integer.parseInt(intWritable.toString());
        if (keyInt > 20000){
            return 0;
        }else if (keyInt > 10000){
            return 1;
        }else{
            return 2;
        }
    }
}
