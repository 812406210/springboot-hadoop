package com.hadoop.demo.hadoop.reducer;

import com.hadoop.demo.hadoop.entity.OrderGroup;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yangwj
 * @date 2020/6/6 13:02
 */
public class OrderTopNReducer extends Reducer<OrderGroup, NullWritable,OrderGroup,NullWritable> {
    @Override
    protected void reduce(OrderGroup key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> it = values.iterator();
        int i = 0;
        while (it.hasNext()){
            NullWritable ni = it.next();
            i++;
            if(i == 3){ break; }
            context.write(key,ni);
        }
    }
}
