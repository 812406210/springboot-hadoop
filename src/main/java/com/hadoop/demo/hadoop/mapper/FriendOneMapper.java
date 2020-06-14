package com.hadoop.demo.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/7 9:58
 */
public class FriendOneMapper extends Mapper<LongWritable,Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] friends = value.toString().split(":");
        String user = friends[0];
        String[] f = friends[1].split(",");
        for (String friend:f) {
            k.set(friend);
            v.set(user);
            context.write(k,v);
        }
    }

}
