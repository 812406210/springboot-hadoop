package com.hadoop.demo.hadoop.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author yangwj
 * @date 2020/6/7 10:00
 */
public class FriendTwoReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String  friends = "";
        for (Text value : values) {
            friends = friends+value+",";
        }
        context.write(key,new Text(friends));
    }
}
