package com.hadoop.demo.hadoop.jobsubmmit;

import com.hadoop.demo.hadoop.entity.Flow;
import com.hadoop.demo.hadoop.entity.OrderGroup;
import com.hadoop.demo.hadoop.entity.OrderGroupComparator;
import com.hadoop.demo.hadoop.mapper.FlowCountMapper;
import com.hadoop.demo.hadoop.mapper.OrderTopNMapper;
import com.hadoop.demo.hadoop.partntion.OrderGroupPartitioner;
import com.hadoop.demo.hadoop.reducer.FlowCountReducer;
import com.hadoop.demo.hadoop.reducer.OrderTopNReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author yangwj
 * @date 2020/6/5 21:10
 */
public class OrderGroupJobWindows {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderGroupJobWindows.class);

        job.setMapperClass(OrderTopNMapper.class);
        job.setReducerClass(OrderTopNReducer.class);

        job.setNumReduceTasks(2);
        job.setPartitionerClass(OrderGroupPartitioner.class);
        job.setGroupingComparatorClass(OrderGroupComparator.class);
        job.setMapOutputKeyClass(OrderGroup.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderGroup.class);
        job.setOutputValueClass(NullWritable.class);

        //job.setPartitionerClass(WordCountPartition.class);
        FileInputFormat.setInputPaths(job, new Path("d:/mydata/order/{input/*}"));
        FileOutputFormat.setOutputPath(job, new Path("d:/mydata/order/output"));


        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }




}
