package bigdata.hermesfuxi.datayi.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object CubeDistinctAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val data = spark.createDataset(Seq(
      "1,江苏省,南通市,下关区",
      "1,江苏省,南通市,下关区",
      "2,江苏省,南通市,下关区",
      "2,江苏省,南通市,白领区",
      "3,江苏省,南通市,白领区",
      "3,江苏省,南通市,富豪区",
      "1,江苏省,苏州市,园林区",
      "1,江苏省,苏州市,园林区",
      "4,江苏省,苏州市,虎跳区"
    ))

    val frame = data.map(s => {
      val arr = s.split(",")
      (arr(0), arr(1), arr(2), arr(3))
    }).toDF("guid", "province", "city", "region")

    frame.createTempView("input")
    val result1 = spark.sql(
      """
        |select
        |    province,
        |    city,
        |    region,
        |    collect_set(guid) as guidList
        |from input
        |group by province, city, region
        |
        |""".stripMargin)

    result1.show()
//    +--------+------+------+--------+
//    |province|  city|region|guidList|
//    +--------+------+------+--------+
//    |  江苏省|苏州市|虎跳区|     [4]|
//      |  江苏省|苏州市|园林区|     [1]|
//      |  江苏省|南通市|下关区|  [1, 2]|
//      |  江苏省|南通市|富豪区|     [3]|
//      |  江苏省|南通市|白领区|  [2, 3]|
//      +--------+------+------+--------+

    result1.createTempView("result1")

    val combineUnique = (arrayList: mutable.WrappedArray[mutable.WrappedArray[String]]) => {
      val flattenArr = arrayList.flatten.distinct
      flattenArr
    }
    spark.udf.register("combine_unique", combineUnique);

    spark.sql(
      """
        |select
        |    province,
        |    combine_unique(collect_set(guidList)) as newGuidList
        |from result1
        |group by province
        |
        |""".stripMargin).show()

//    +--------+------------+
//    |province| newGuidList|
//    +--------+------------+
//    |  江苏省|[3, 2, 4, 1]|
//      +--------+------------+

    spark.close()

  }
}
