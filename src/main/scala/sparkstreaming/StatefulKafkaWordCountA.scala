package sparkstreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author yangwj
 * @date 2020/7/16 21:03
 * @version 1.0
 * @desc 累计历史结果
 */
object StatefulKafkaWordCountA {

  /**
   * 第一个参数：聚合的key,就是单词
   * 第二个参数：当前批次产生批次单词出现的次数
   * 第三个参数：初始值或累加的中间结果
   */
  val updateFunc = (iterator:Iterator[(String,Seq[Int],Option[Int])]) => {
      iterator.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StatefulKafkaWordCountA").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))
    //如果要使用更新的历史纪录(累加)，那么就要把终结结果保存起来
    ssc.checkpoint("hdfs://hadoop:9000/")
    //kafka参数
    val zkQuorum = ""
    val grooupId = ""
    val topic = Map[String,Int]("TEST" ->1)
    //创建DStream,需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, grooupId, topic)
    //kafka的ReceiverInputDStream[(String, String)]里面装的是一个元组(key是写入的key,value是数据部分)
    val lines: DStream[String] = data.map(_._2)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //调用updateStateByKey，累加历史结果
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc ,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    result.print()
    ssc.start()
    
    ssc.awaitTermination()
  }

}
