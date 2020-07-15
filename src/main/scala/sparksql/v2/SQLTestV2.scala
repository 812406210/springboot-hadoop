package sparksql.v2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * @author yangwj
 * @date 2020/7/12 15:37
 * @version 1.0
 * @desc spark2.x的编程api(sparkSession)
 */
object SQLTestV2 {
  def main(args: Array[String]): Unit = {
    //spark2.x的编程api(sparkSession)
    //是spark2.x SQL执行的入口
    val session: SparkSession = SparkSession.builder().master("local[*]")
                                .appName("SQLTestV2").config("", "").getOrCreate()
    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("F:\\spart_data\\person\\person.txt")
    //整理数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val fileds = line.split(",")
      val id: Long = fileds(0).toLong
      val name: String = fileds(1)
      val age: Int = fileds(2).toInt
      val fv: Double = fileds(3).toDouble
      Row(id,name,age,fv)
    })
    //结果类型，其实就是表头，用于描述DataFrame
    var schema = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("fv",DoubleType,true)
    ))
    //创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)
    import session.implicits._
    //df.select($"name",$"age",$"fv")
    //df.filter("")
    val df1: Dataset[Row] = df.where($"fv" > 20).orderBy($"fv" desc)
    df1.show()
    session.stop()
  }

}
