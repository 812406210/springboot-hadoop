package sparkcore.case2

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{BufferedSource, Source}

/**
 * @author yangwj
 * @date 2020/7/11 21:12
 * @version 1.0
 */
object MyUtils {

  def ip2Long(ip:String): Long ={
      val fragments = ip.split("[.]")
      var ipNum = 0L
      for(i <- 0 until fragments.length){
          ipNum = fragments(i).toLong | ipNum <<8L
      }
    ipNum
  }

  def readRules(path:String):Array[(Long,Long,String)] = {
    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()

    //将ip规则进行整理,如果是集群，需要进行广播，否则其他机器无法使用(并放入内存中)
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val startNum: Long = fields(2).toLong
      val endNum: Long = fields(3).toLong
      val province: String = fields(6)
      (startNum, endNum, province)
    }).toArray
    // ruturn
    rules
  }

  //二分法查找
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def data2MySQL(it: Iterator[(String, Int)]): Unit = {
    //一个迭代器代表一个分区，分区中有多条数据
    //先获得一个JDBC连接
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123568")
    //将数据通过Connection写入到数据库
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
    //将分区中的数据一条一条写入到MySQL中
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    //将分区中的数据全部写完之后，在关闭连接
    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {1
    val conf: SparkConf = new SparkConf().setAppName("IpLocation").setMaster("local")
    val sc = new SparkContext(conf)
    //在Driver端获取全部的IP规则数据(全部的IP规则数据在某一台机器上，跟Driver在同一台机器上)
    val rules: Array[(Long, Long, String)] = readRules("F:\\spart_data\\ip\\ip.txt")

    val ipNum = ip2Long("1.1.1.1")
    val index: Int = binarySearch(rules, ipNum)
    val tp: (Long, Long, String) = rules(index)
    val province: String = tp._3
    print(province)

  }
}
