package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.entity.Flow;
import com.hadoop.demo.hadoop.mapper.FlowCountMapper;
import com.hadoop.demo.hadoop.mapper.WordCountMapper;
import com.hadoop.demo.hadoop.partntion.WordCountPartition;
import com.hadoop.demo.hadoop.reducer.FlowCountReducer;
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
public class FlowJobWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowJobWindows.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);

        //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/flow/{input/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/flow/output"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
