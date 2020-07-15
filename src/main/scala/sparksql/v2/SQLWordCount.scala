package sparksql.v2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author yangwj
 * @date 2020/7/12 15:37
 * @version 1.0
 * @desc spark2.x的编程api(sparkSession)
 */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    //spark2.x的编程api(sparkSession)
    //是spark2.x SQL执行的入口
    val session: SparkSession = SparkSession.builder().master("local[*]")
                                .appName("SQLWordCount").config("", "").getOrCreate()
    // 读数据，创建RDD，是lazy
    //val lines: RDD[String] = session.sparkContext.textFile("F:\\spart_data\\person\\person.txt")
    //Dataset 分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    val lines: Dataset[String] = session.read.textFile("F:\\spart_data\\person\\person.txt")
    //需要导入隐士转换
    import  session.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(","))

    //注册视图
    words.createTempView("v_wc")
    val df: DataFrame = session.sql("select value,count(*) from v_wc group by value")
    df.show()
    session.stop()

  }

}
