package sparksql.v2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import sparkcore.case2.MyUtils

/**
 * @author yangwj
 * @date 2020/7/12 8:50
 * @version 1.0
 * @desc join的代价太昂贵，而且非常慢，解决思路是：将小表缓存起来(广播变量)
 */
object IpLoaction2Join {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("IpLoaction2Join").master("local[*]").getOrCreate()

    //取到HDFS中的ip规则
    val ruleLines: Dataset[String] = session.read.textFile("F:\\spart_data\\ip\\ip.txt")
    //整理ip规则数据
    import  session.implicits._
    val ruleDF: DataFrame = ruleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("sNum","eNum","province")

    //创建RDD，读取访问日志
    val accessLinnes: Dataset[String] = session.read.textFile("F:\\spart_data\\ip\\access.log")
    //整理数据
    val ipDF: DataFrame = accessLinnes.map(log => {
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum: Long = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    //创建视图
    ruleDF.createTempView("v_rules")
    ipDF.createTempView("v_ip")

    val result: DataFrame = session.sql("select province ,count(*) counts from v_ip join v_rules on (ip_num >= sNum) and  (ip_num <= eNum) group by province order by counts desc")
    result.show()
    session.stop()

  }


}
