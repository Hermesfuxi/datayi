import bigdata.hermesfuxi.datayi.functions.{BitmapOrAggregation, BitmapOrAggregationFunction}
import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil
import org.apache.spark.sql.SparkSession

object BitMapAggregationTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
//      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

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
      (Integer.valueOf(arr(0)), arr(1), arr(2), arr(3))
    }).toDF("guid", "province", "city", "region")

    frame.createTempView("input")
    spark.udf.register("to_bitmap", RoaringBitmapUtil.arrayToBitmap)
    val result1 = spark.sql(
      """
        |select
        |    province,
        |    city,
        |    region,
        |    to_bitmap(collect_set(guid)) as guid_bitmap
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

//    spark.udf.register("bitmap_or_agg", BitmapOrAggregationFunction)
    spark.udf.register("bitmap_or_agg", udaf(BitmapOrAggregation))
    spark.udf.register("bitmap_card", RoaringBitmapUtil.getCardNum _)
    spark.sql(
      """
        |select
        |    province,
        |    bitmap_card(bitmap_or_agg(guid_bitmap)) as guid_cn
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
