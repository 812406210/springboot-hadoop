package com.hadoop.demo.hadoop.sequence;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author yangwj
 * @date 2020/6/5 21:10
 */
public class IndexCountJobStepOneWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexCountJobStepOneWindows.class);

        job.setMapperClass(IndexCountStepOneMapper.class);
        job.setReducerClass(IndexCountStepOneReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(TextOutputFormat.class); //默认的输出文本格式
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/index/{input/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/index/output-sequence"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
