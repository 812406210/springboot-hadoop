package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.mapper.FriendOneMapper;
import com.hadoop.demo.hadoop.mapper.FriendTwoMapper;
import com.hadoop.demo.hadoop.reducer.FriendOneReducer;
import com.hadoop.demo.hadoop.reducer.FriendTwoReducer;
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
public class FriendsTwoJobWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);

        job.setJarByClass(FriendsTwoJobWindows.class);

        job.setMapperClass(FriendTwoMapper.class);
        job.setReducerClass(FriendTwoReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/friends/{output/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/friends/output1"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
