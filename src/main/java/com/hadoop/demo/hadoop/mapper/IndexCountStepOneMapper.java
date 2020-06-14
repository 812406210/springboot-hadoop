package com.hadoop.demo.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/6 10:17
 */
public class IndexCountStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取分片信息
        FileSplit is = (FileSplit) context.getInputSplit();
        String fileName = is.getPath().getName();
        String[] words = value.toString().split(" ");
        for (String word : words) {
            context.write(new Text(word+"-"+fileName),new IntWritable(1));
        }


    }
}
