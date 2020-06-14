package com.hadoop.demo.hadoop.sequence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/6 10:17
 */
public class IndexCountStepTwoMapper extends Mapper<Text, IntWritable, Text, Text> {
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        //获取分片信息
        String[] words = value.toString().split("-");

        context.write(new Text(words[0]),new Text(words[1]));

    }
}
