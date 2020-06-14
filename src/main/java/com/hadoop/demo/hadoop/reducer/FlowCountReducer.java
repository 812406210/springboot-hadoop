package com.hadoop.demo.hadoop.reducer;

import com.hadoop.demo.hadoop.entity.Flow;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yangwj
 * @date 2020/6/5 16:19
 */
public class FlowCountReducer extends Reducer<Text, Flow,Text,Flow> {
    private Flow sumFlow = new Flow();

    @Override
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
        Long sumUpStream = 0L;
        Long sumDownStream = 0L;
        Long sumSumStream = 0L;
        Iterator<Flow> it =  values.iterator();
        while (it.hasNext()){
            Flow flow = it.next();
            sumUpStream +=flow.getUpStream();
            sumDownStream +=flow.getDownStream();
            sumSumStream +=flow.getSumStream();
        }
        sumFlow.setUpStream(sumUpStream);
        sumFlow.setDownStream(sumDownStream);
        sumFlow.setSumStream(sumSumStream);
        context.write(key,sumFlow);
    }
}
