package com.hadoop.demo.hadoop.mapper;

import com.hadoop.demo.hadoop.entity.Flow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/5 16:09
 */
public class FlowCountMapper extends Mapper<LongWritable, Text,Text, Flow> {
    private Flow flow = new Flow();
    private Text phone = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");
        Long upStream =Long.parseLong(words[words.length-3]);
        Long downStream =Long.parseLong(words[words.length-2]);
        phone.set(words[1]);
        flow.setUpStream(upStream);
        flow.setDownStream(downStream);
        flow.setSumStream(upStream+downStream);
        context.write(phone,flow);

    }
}
