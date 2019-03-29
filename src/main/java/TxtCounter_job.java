import completesort.CompleteSortMapper;
import completesort.CompleteSortReducer;
import completesort.MySort;
import exam6.SVGMap;
import exam6.SVGMapEx;
import exam6.SVGReducer;
import exam6.SVGReducerEx;
import join.AJoinMapper;
import join.BJoinMapper;
import join.JoinReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
//import org.mockito.internal.matchers.Null;
import secondsort.IntPair;
import secondsort.MyKeyGroupComparator;
import secondsort.SecondMapper;
import secondsort.SecondReducer;
import task.*;
import topn.Top10Mapper;
import topn.Top10Reducer;
import topn.Top10Writeable;
import utils.MyPartitioner;
import utils.TxtSVG_Writable;


import java.io.IOException;

public class TxtCounter_job {


    public static void test1(String input,String output){
        //String inputPath = "hdfs://127.0.0.1:9000/user/wordcount/";
        //String outputPath = "hdfs://127.0.0.1:9000/user/wordcount-output/";
        //args = new String[]{inputPath,outputPath};
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(WordCountMap.class);
            job.setReducerClass(WordCountReducer.class);
            job.setCombinerClass(WordCountReducer.class);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    public static void test2(){
        //String inputPath = "hdfs://127.0.0.1:9000/user/wordcount/";
        //String outputPath = "hdfs://127.0.0.1:9000/user/wordcount-output/";
        //args = new String[]{inputPath,outputPath};
        Configuration conf = new Configuration();
        String[] otherArgs = new String[] {"/input1","/amulti"};
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.setJobName("Single Table Join");
            job.setNumReduceTasks(1);
            MultipleInputs.addInputPath(job, new Path("/inputA"), TextInputFormat.class, MapTaskBlank.class);
            MultipleInputs.addInputPath(job, new Path("/inputB"),TextInputFormat.class, MapTaskComma.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //job.setMapperClass(WordCountMap.class);
            job.setReducerClass(ReducerMultiInput.class);

            FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void test3(String input,String output){
        //String inputPath = "hdfs://127.0.0.1:9000/user/wordcount/";
        //String outputPath = "hdfs://127.0.0.1:9000/user/wordcount-output/";
        //args = new String[]{inputPath,outputPath};
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(SVGMap.class);
            job.setReducerClass(SVGReducer.class);
            //job.setCombinerClass(WordCountReducer.class);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void test4(String input,String output){
        //String inputPath = "hdfs://127.0.0.1:9000/user/wordcount/";
        //String outputPath = "hdfs://127.0.0.1:9000/user/wordcount-output/";
        //args = new String[]{inputPath,outputPath};
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(TxtSVG_Writable.class);

            job.setMapperClass(SVGMapEx.class);
            job.setReducerClass(SVGReducerEx.class);
            job.setCombinerClass(SVGReducerEx.class);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void test5(String input,String output){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(WordCountMap.class);
            job.setReducerClass(WordCountReducer.class);
            //job.setCombinerClass(WordCountReducer.class);
            job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(4);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void testTop10(String input,String output){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);

            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Top10Writeable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(Top10Mapper.class);
            job.setReducerClass(Top10Reducer.class);
            //job.setCombinerClass(WordCountReducer.class);
            //job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void CompleteSort(String input,String output){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(NullWritable.class);

            job.setMapperClass(CompleteSortMapper.class);
            job.setReducerClass(CompleteSortReducer.class);
            job.setPartitionerClass(completesort.MyPartitioner.class);
            job.setSortComparatorClass(MySort.class);
            //job.setCombinerClass(WordCountReducer.class);
            //job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(3);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void TotalOrderSort(String input,String output){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);


            InputSampler.RandomSampler<Text,Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 1000,10);
            Path partitionFile = new Path("_partitions");
            TotalOrderPartitioner.setPartitionFile(conf,partitionFile);


            job.setMapperClass(Mapper.class);
            job.setReducerClass(Reducer.class);
            job.setPartitionerClass(TotalOrderPartitioner.class);
            job.setSortComparatorClass(MySort.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //job.setCombinerClass(WordCountReducer.class);
            //job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(3);

            //Path path = new Path("sortoutput1");
            InputSampler.writePartitionFile(job,sampler);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void SecondSort(String input,String output){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);

            job.setMapOutputKeyClass(IntPair.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(SecondMapper.class);
            job.setReducerClass(SecondReducer.class);
            job.setGroupingComparatorClass(MyKeyGroupComparator.class);
            job.setSortComparatorClass(MySort.class);
            //job.setCombinerClass(WordCountReducer.class);
            //job.setPartitionerClass(MyPartitioner.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void joinDemo(String input1,String input2,String output,String jointype){
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);
            job.setJarByClass(TxtCounter_job.class);
            job.getConfiguration().set("join.type",jointype);


            MultipleInputs.addInputPath(job,new Path(input1),TextInputFormat.class, AJoinMapper.class);
            MultipleInputs.addInputPath(job,new Path(input2),TextInputFormat.class, BJoinMapper.class);
            job.setReducerClass(JoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //job.setCombinerClass(WordCountReducer.class);
            //job.setPartitionerClass(MyPartitioner.class);
            //job.setNumReduceTasks(1);

            //FileInputFormat.addInputPath(job,new Path(input));
            FileOutputFormat.setOutputPath(job,new Path(output));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        joinDemo(args[0],args[1],args[2],args[3]);

    }
}
