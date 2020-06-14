package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.combiner.WordCountCombiner;
import com.hadoop.demo.hadoop.mapper.WordCountMapper;
import com.hadoop.demo.hadoop.partntion.WordCountPartition;
import com.hadoop.demo.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author yangwj
 * @date 2020/6/5 21:10
 */
public class WordCountWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountWindows.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountCombiner.class); //map端进行聚合操作
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/wordcount/{input/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/wordcount/output"));

        job.setNumReduceTasks(3);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
