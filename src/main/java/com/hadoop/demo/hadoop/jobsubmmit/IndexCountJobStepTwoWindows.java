package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.mapper.IndexCountStepTwoMapper;
import com.hadoop.demo.hadoop.reducer.IndexCountStepTwoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author yangwj
 * @date 2020/6/5 21:10
 */
public class IndexCountJobStepTwoWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS", "file:///");
//        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexCountJobStepTwoWindows.class);

        job.setMapperClass(IndexCountStepTwoMapper.class);
        job.setReducerClass(IndexCountStepTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

          //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/index/{output1/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/index/output2"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
