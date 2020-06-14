package com.hadoop.demo.hadoop.sequence;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yangwj
 * @date 2020/6/6 10:33
 */
public class IndexCountStepOneReducer extends Reducer<Text, IntWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> it = values.iterator();
       while (it.hasNext()){
           IntWritable value = it.next();
           count += value.get();
       }
       String[] keys = key.toString().split("-");
       context.write(new Text(keys[0]),new Text(keys[1]+"-->"+count));
    }
}
