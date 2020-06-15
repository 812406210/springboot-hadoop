package com.hadoop.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author yangwj
 * @date 2020/6/15 21:15
 */
public class LambdaWordCount {
    public static void main(String[] args) {
        //创建4个线程进行
//        SparkConf conf = new SparkConf().setAppName("JavaWorldCount").setMaster("local[4]");

        SparkConf conf = new SparkConf().setAppName("JavaWorldCount").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取数据
        JavaRDD<String> lines = jsc.textFile("D:\\1.txt");

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));

        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);

        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        JavaPairRDD<String, Integer> result  = sorted.mapToPair(tp -> tp.swap());
        result.saveAsTextFile("");

        jsc.stop();
    }
}
