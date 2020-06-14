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
public class FriendOneReducer extends Reducer<Text,Text,Text,Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> userList = new ArrayList<>();

        for (Text value : values) {
            userList.add(value.toString());
        }
        Collections.sort(userList);

        for(int i =0 ; i<userList.size()-1;i++){
            for(int j = i+1;j<userList.size();j++){
                context.write(new Text(userList.get(i)+"-"+userList.get(j)),key);
            }
        }

    }
}
