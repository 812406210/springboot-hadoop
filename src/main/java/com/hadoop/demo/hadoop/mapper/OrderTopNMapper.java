package com.hadoop.demo.hadoop.mapper;

import com.hadoop.demo.hadoop.entity.OrderGroup;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yangwj
 * @date 2020/6/6 12:55
 */
public class OrderTopNMapper extends Mapper<LongWritable, Text, OrderGroup, NullWritable> {
    OrderGroup orderGroup = new OrderGroup();
    NullWritable v = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        orderGroup.set(fields[0],fields[1],fields[2],Float.parseFloat(fields[3]),Integer.parseInt(fields[4]));
        context.write(orderGroup,v);
    }
}
