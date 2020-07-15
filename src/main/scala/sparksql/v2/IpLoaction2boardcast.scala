package sparksql.v2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import sparkcore.case2.MyUtils

/**
 * @author yangwj
 * @date 2020/7/12 8:50
 * @version 1.0
 * @desc join的代价太昂贵，而且非常慢，解决思路是：将小表缓存起来(广播变量)
 */
object IpLoaction2boardcast {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName("IpLoaction2Join").master("local[*]").getOrCreate()

    //取到HDFS中的ip规则
    val ruleLines: Dataset[String] = session.read.textFile("F:\\spart_data\\ip\\ip.txt")
    //整理ip规则数据
    import session.implicits._
    val rulesDataSet= ruleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //收集ip规则到Driver端
   val rulesInDriver: Array[(Long, Long, String)] = rulesDataSet.collect()
    //将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = session.sparkContext.broadcast(rulesInDriver)

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
    ipDF.createTempView("v_log")

    //定义一个自定义函数 UDF,并注册
    session.udf.register("ip2Province",(ipNum:Long)=>{
       //查找ip规则，(事先已经广播了，已经在excetor中了)
      //使用广播引用，就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据ip地址的十进制查找省份名字
      val index: Int = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1){
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    val result: DataFrame = session.sql("select ip2Province(ip_num) province count(*) counts from v_log group by province order by counts desc ")
    result.show()
    session.stop()
  }


}
