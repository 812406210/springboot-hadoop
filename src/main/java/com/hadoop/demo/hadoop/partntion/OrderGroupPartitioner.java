package com.hadoop.demo.hadoop.partntion;

import com.hadoop.demo.hadoop.entity.OrderGroup;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangwj
 * @date 2020/6/6 13:09
 */
public class OrderGroupPartitioner extends Partitioner<OrderGroup, NullWritable> {
    @Override
    public int getPartition(OrderGroup orderGroup, NullWritable nullWritable, int i) {
        //按照订单中的orderId来分发数据
        return (orderGroup.getOrderId().hashCode() & Integer.MAX_VALUE)% i;
    }
}
