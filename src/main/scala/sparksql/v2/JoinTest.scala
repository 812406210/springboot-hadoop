package sparksql.v2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author yangwj
 * @date 2020/7/12 21:22
 * @version 1.0
 */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import session.implicits._
    val lines: Dataset[String] = session.createDataset(List("1,laoyang,china", "2,laozhou,US"))
    //对数据进行整理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val nationCode: String = fields(2)
      (id, name, nationCode)
    })
    val df1: DataFrame = tpDs.toDF("id", "name", "nation")


    val nations: Dataset[String] = session.createDataset(List("china,中国", "US,美国"))
    //对数据进行整理
    val ndataset: Dataset[(String, String)] = nations.map(line => {
      val fields: Array[String] = line.split(",")
      val ename: String = fields(0)
      val cname: String = fields(1)
      (ename, cname)
    })
    val df2: DataFrame = ndataset.toDF("ename", "cname")

    //第一种，创建视图
    df1.createTempView("v_users")
    df2.createTempView("v_nations")
    val result: DataFrame = session.sql("select name,cname from v_users join v_nations on v_users.nation = v_nations.ename ")
    result.show()

    //第二种 使用api的join方法
    val result2= df1.join(df2, $"nation" === $"ename", "outer")
    result2.show()
    session.stop()

  }
}
