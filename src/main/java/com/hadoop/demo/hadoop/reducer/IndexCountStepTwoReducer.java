package com.hadoop.demo.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yangwj
 * @date 2020/6/6 10:33
 */
public class IndexCountStepTwoReducer extends Reducer<Text, Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String v = "";
        Iterator<Text> it = values.iterator();
        while (it.hasNext()){
            Text value = it.next();
            v =v+value.toString()+ ",";
        }
        context.write(key,new Text(v));
    }
}
