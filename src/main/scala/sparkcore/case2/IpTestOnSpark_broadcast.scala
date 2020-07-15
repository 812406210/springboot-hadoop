package sparkcore.case2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangwj
 * @date 2020/7/11 21:12
 * @version 1.0
 */
object IpTestOnSpark_broadcast {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpTestOnSpark").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    //arg(0) ip规则文件
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //将Driver端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用(还在Driver端)
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建RDD，读取访问日志 arg(1) 访问日志文件
    val accessLines: RDD[String] = sc.textFile(args(1))

    //整理数据
    val func = (line:String) => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转为10进制
      val ipNum: Long = MyUtils.ip2Long(ip)

      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      val index: Int = MyUtils.binarySearch(rulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1) {
        val tp: (Long, Long, String) = rulesInExecutor(index)
        province = tp._3
      }
      (province,1)
    }

    //将ip地址转为十进制
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)
    //将结果打印
    val result: Array[(String, Int)] = reduced.collect()

    print(result.toBuffer)

  }
}
