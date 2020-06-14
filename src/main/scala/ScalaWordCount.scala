import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author yangwj
  * @date 2020/6/14 22:32
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCountApp").setMaster("local")

    val sc = new SparkContext(conf);
    val inputFile =  "D:\\1.txt"
    val lines: RDD[String] =  sc.textFile(inputFile);
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
