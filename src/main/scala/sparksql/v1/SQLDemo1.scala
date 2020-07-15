package sparksql.v1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author yangwj
 * @date 2020/7/12 10:52
 * @version 1.0
 * @desc 普通RDD转换成DataFrame
 */
object SQLDemo1 {

  def main(args: Array[String]): Unit = {
    //提交的程序可以连接到spark集群中
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("local")

    //创建sparkSQL的连接
    val sc = new SparkContext(conf)

    //SparkContext不能创建特殊的RDD(DataFrame),需要对SparkContext进行增强
    val sQLContext = new SQLContext(sc)

    //先有一个普通的RDD，然后在关联上schema，最后将普通RDD转换成DataFrame
    val lines: RDD[String] = sc.textFile("F:\\spart_data\\person\\person.txt")
    val boyRDD: RDD[Boy] = lines.map(line => {
      val fileds = line.split(",")
      val id: Long = fileds(0).toLong
      val name: String = fileds(1)
      val age: Int = fileds(2).toInt
      val fv: Double = fileds(3).toDouble
      Boy(id, name, age, fv)
    })
    //boyRDD是一个普通RDD，尽管关联上了schema,但是还是普通的RDD
    //需要将boyRDD进行转换成DataFrame,可以通过隐士转换，即toDF()(注：转换前，需要先导包import sQLContext.implicits._)
    import sQLContext.implicits._
    val bdf: DataFrame = boyRDD.toDF()

    //变成DF后可以使用两种API进行编程了，需要把DataFrame先注册为临时表，然后才可以使用sql进行编码
    bdf.registerTempTable("t_boy")

    //书写sql,SQL方法其实是Transformation操作
    val allData: DataFrame = sQLContext.sql("select * from t_boy where age > 20")
    //触发action,查看结果
    allData.show()
    sc.stop()
  }
}

//schema信息
case class Boy(id:Long,name:String,age:Int,fv:Double)
