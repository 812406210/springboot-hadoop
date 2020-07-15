package sparkcore.case1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangwj
 * @date 2020/7/8 21:30
 * @version 1.0
 */
object FavTeacher {

  def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
      val sc = new SparkContext(conf)

      //读取数据
      val lines :RDD[String] = sc.textFile("F:\\spart_data\\topN\\input\\teacher.log");

      //数据处理
      val teacherAndOne = lines.map(line => {
        val index = line.lastIndexOf("/")
        val teacher = line.substring(index + 1)
        //val httpHost = line.substring(0, index)
        //val subject = new URL(httpHost).getHost.split("[.]")(0)
        (teacher, 1)
      })

      //聚合
      val reduced:RDD[(String,Int)] = teacherAndOne.reduceByKey(_+_)

      //排序
      val sorted: RDD[(String,Int)] = reduced.sortBy(_._2,false);

      val result :Array[(String,Int)] = sorted.collect();

      //打印
      println(result.toBuffer);

      sc.stop();


  }
}
