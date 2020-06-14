package com.hadoop.demo.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/7 9:58
 */
public class FriendTwoMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] friends = value.toString().split("\t");
        String user = friends[0];
        String friend = friends[1];
        context.write(new Text(user),new Text(friend));
    }

}
