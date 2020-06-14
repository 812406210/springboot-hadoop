package com.hadoop.demo.hadoop.entity;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author yangwj
 * @date 2020/6/6 12:52
 */
public class OrderGroupComparator extends WritableComparator {

    //反序列化
    public OrderGroupComparator(){
        super(OrderGroup.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderGroup o1 = (OrderGroup) a;
        OrderGroup o2 = (OrderGroup) b;
        return o1.getOrderId().compareTo(o2.getOrderId());
    }
}
