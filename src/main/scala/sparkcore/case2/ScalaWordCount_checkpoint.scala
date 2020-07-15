package sparkcore.case2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yangwj
  * @date 2020/6/14 22:32
  */
object ScalaWordCount_checkpoint {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCountApp").setMaster("local")

    val sc = new SparkContext(conf);

    //checkpoint 操作,创建checkpoint目录，存放在hdfs中
    sc.setCheckpointDir("hdfs://hadoop100:9000/ch20200712")
    val inputFile =  "D:\\1.txt"
    val lines: RDD[String] =  sc.textFile(inputFile);

    //将计算后的结果进行checkpoint保存,注意：checkpoint必须要action动作才会触发
    lines.checkpoint()

    //注意：如果开始了cache,又checkpoint，会优秀执行cache
    val words: RDD[(String)] = lines.flatMap(_.split(" "));

    val wordAndOne:RDD[(String, Int)] =  words.map((_,1));
    //按key进行聚合
    val reduced:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    sorted.foreach(println);
    //释放资源
    sc.stop()


  }

}
