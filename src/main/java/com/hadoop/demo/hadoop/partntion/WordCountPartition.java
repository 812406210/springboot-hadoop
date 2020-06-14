package com.hadoop.demo.hadoop.partntion;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangwj
 * @date 2020/6/5 21:35
 */
public class WordCountPartition extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        String flag = text.toString();
        if(flag.startsWith("h")){
            return 0;
        }else if(flag.startsWith("y") || flag.startsWith("z")){
            return 1;
        }else {
            return 2;
        }

    }
}
