package sparkcore.case1

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD



/**
 * @author yangwj
 * @date 2020/7/8 22:43
 * @version 1.0
 */
object GroupFavTeacher1 {
  /**
   * 数据量过大，不建议使用这种方式，会出现内存溢出
   * @param args
   */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

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

    //按学科分组 【学科，学科对应的老师数据】
    val grouped: RDD[(String,Iterable[((String,String),Int)])] = reduced.groupBy(_._1._1)

    //排序
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(2))
    //ctrl + alt + v代码补全
    val result :Array[(String, List[((String, String), Int)])] = sorted.collect();

    //打印
    println(result.toBuffer);

    sc.stop();
  }
}
