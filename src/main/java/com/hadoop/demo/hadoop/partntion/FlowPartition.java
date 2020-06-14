package com.hadoop.demo.hadoop.partntion;

import com.hadoop.demo.hadoop.entity.Flow;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yangwj
 * @date 2020/6/5 17:08
 */
public class FlowPartition extends Partitioner<Text, Flow> {
    /**
     * 数据分发规则
     * @param text
     * @param flow
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, Flow flow, int i) {
        String phone = text.toString().substring(0,3);
        switch (phone){
            case "123":
                return 0;
            case "456":
                return 1;
            case "789":
                return 2;
            default:
                return 4;
        }
    }
}
