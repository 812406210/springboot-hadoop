package com.hadoop.demo.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;


/**
 * @author yangwj
 * @date 2020/6/3 20:41
 * LongWritable,Text,Text , IntWritable
 * LongWritable 读取文件的偏移量类型
 * Text 读取的一行的数据了些
 * Text 输出的单词数据类型
 * IntWritable 输出的单词数量类型
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text , IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、切词
        System.out.println(key);
        String[] words = value.toString().split(" ");
        for (String word:words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
