package com.hadoop.demo.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yangwj
 * @date 2020/6/3 20:56
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        int count = 0;
        Iterator<IntWritable>  it= values.iterator();
        while (it.hasNext()){
           IntWritable value = it.next();
           count +=value.get();
        }

        context.write(key,new IntWritable(count));
    }
}
