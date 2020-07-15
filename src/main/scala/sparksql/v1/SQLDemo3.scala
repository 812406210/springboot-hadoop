package sparksql.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangwj
 * @date 2020/7/12 10:52
 * @version 1.0
 * @desc 使用DataFrame Api
 */
object SQLDemo3 {

  def main(args: Array[String]): Unit = {
    //提交的程序可以连接到spark集群中
    val conf = new SparkConf().setAppName("SQLDemo3").setMaster("local")

    //创建sparkSQL的连接
    val sc = new SparkContext(conf)

    //SparkContext不能创建特殊的RDD(DataFrame),需要对SparkContext进行增强
    val sQLContext = new SQLContext(sc)

    //先有一个普通的RDD，然后在关联上schema，最后将普通RDD转换成DataFrame
    val lines: RDD[String] = sc.textFile("F:\\spart_data\\person\\person.txt")
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
    val bdf: DataFrame = sQLContext.createDataFrame(rowRDD, schema)

    //不使用SQL方式，就不需要注册临时表了....
    val df1: DataFrame = bdf.select("name", "age", "fv")
    import  sQLContext.implicits._
    val df2: DataFrame = df1.orderBy($"age" desc)

    //书写sql,SQL方法其实是Transformation操作
    //val allData: DataFrame = sQLContext.sql("select * from t_boy where age > 20")
    //触发action,查看结果
    df2.show()
    sc.stop()
  }
}
