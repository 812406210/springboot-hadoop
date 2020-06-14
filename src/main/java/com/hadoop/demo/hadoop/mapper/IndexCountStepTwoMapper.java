package com.hadoop.demo.hadoop.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/6 10:17
 */
public class IndexCountStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取分片信息
        String[] words = value.toString().split("\t");

        context.write(new Text(words[0]),new Text(words[1]));

    }
}
