package utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text,IntWritable>{

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        if (text.toString().equals("bye")){
            return 0;
        }else if (text.toString().equals("hello")){
            return 1;
        }else if (text.toString().equals("hadoop")){
            return 2;
        }
        return 3;
    }
}
