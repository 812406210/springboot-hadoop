package sparkcore.case1

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author yangwj
 * @date 2020/7/8 22:43
 * @version 1.0
 */
object GroupFavTeacher2 {

  /**
   * 防止内存溢出，可以分开提交到集群
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val subjects = Array("javaee","bigdata","php")

    //读取数据
    val lines :RDD[String] = sc.textFile("F:\\spart_data\\topN\\input\\teacher.log");

    //数据处理
    val subjectTeacherAndOne:RDD[((String,String),Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher),1)
    })

    //聚合
    val reduced:RDD[((String,String),Int)] = subjectTeacherAndOne.reduceByKey(_+_)

    reduced.cache()
    //循环，进行单个的排序
    for (sub <-subjects) {
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sub)
      //take 是一个action,所以每次触发一个take都会提交一个任务到集群
      val favTeacher = filtered.sortBy(_._2, false).take(3);
      //打印
      println(favTeacher.toBuffer);
    }

    sc.stop();
  }
}
