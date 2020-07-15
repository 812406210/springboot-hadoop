package sparksql.v2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author yangwj
 * @date 2020/7/12 15:37
 * @version 1.0
 * @desc spark2.x的编程api(sparkSession)
 */
object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    //spark2.x的编程api(sparkSession)
    //是spark2.x SQL执行的入口
    val session: SparkSession = SparkSession.builder().master("local[*]")
                                .appName("DataSetWordCount").config("", "").getOrCreate()
    // 读数据，创建RDD，是lazy
    //val lines: RDD[String] = session.sparkContext.textFile("F:\\spart_data\\person\\person.txt")
    //Dataset 分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    val lines: Dataset[String] = session.read.textFile("F:\\spart_data\\person\\person.txt")
    //需要导入隐士转换
    import session.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(","))
    //使用DataSet Api
    //val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)

    import  org.apache.spark.sql.functions._
    val result: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)
    result.show()
    session.stop()
  }

}
