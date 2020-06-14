package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.mapper.HitTimesMapper;
import com.hadoop.demo.hadoop.reducer.HitTimesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.jca.cci.CciOperationNotSupportedException;

/**
 * @author yangwj
 * @date 2020/6/5 21:10
 */
public class HitTimesJobWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        //使用配置文件
        conf.addResource("core-map.xml");
        //conf.setInt("top.n",5);
        Job job = Job.getInstance(conf);

        job.setJarByClass(HitTimesJobWindows.class);

        job.setMapperClass(HitTimesMapper.class);
        job.setReducerClass(HitTimesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/hit/{input/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/hit/output"));

        job.setNumReduceTasks(1);

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
