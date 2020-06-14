package com.hadoop.demo.hadoop.reducer;

import com.hadoop.demo.hadoop.entity.HitTopN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @author yangwj
 * @date 2020/6/5 23:10
 */
public class HitTimesReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    //排序
    TreeMap<HitTopN,Object> countMap = new TreeMap(new Comparator<HitTopN>() {
        @Override
        public int compare(HitTopN o1, HitTopN o2) {
            if(o1.getTimes() == o2.getTimes()){
                return 1;
            }
            return o2.getTimes()-o1.getTimes();
        }

    });
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int count = 0 ;
       Iterator<IntWritable> it =  values.iterator();
       while (it.hasNext()){
           IntWritable i = it.next();
           count +=i.get();
       }
       HitTopN top = new HitTopN();
       top.setTimes(count);
       top.setUrl(key.toString());
       countMap.put(top,null);
       //context.write(key,new IntWritable(count));
    }

    /**
     * 这个方法在进行reducer聚合之后会进行调用，所以我们可以在这个方法进行
     * 其他的操作
    * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int s = 0;
        int topN= Integer.parseInt(conf.get("topN"));

        for (Map.Entry<HitTopN, Object> entry : countMap.entrySet()) {
            s++;
            if(s == topN){
                break;
            }
            context.write(new Text(entry.getKey().getUrl()),new IntWritable(entry.getKey().getTimes()));
        }

    }
}
