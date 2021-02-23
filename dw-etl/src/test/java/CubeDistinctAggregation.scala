import bigdata.hermesfuxi.datayi.functions.CombineUnique
import org.apache.spark.sql.SparkSession

object CubeDistinctAggregation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
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

    result1.union(result1)

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

    //    val combineUnique = (arrayList: mutable.WrappedArray[mutable.WrappedArray[String]]) => {
    //      val flattenArr = arrayList.flatten.distinct
    //      flattenArr
    //    }

    //    val combineUnique = new Aggregator[Array[String], Array[String], Array[String]]() {
    //      // 聚合的初始值：比如满足：任何 b + zero = b
    //      override def zero: Array[String] = Array.empty[String]
    //
    //      // 分区内聚合:合并两个值。用新值直接更新buffer(初始值为zero)，并返回buffer本身，而不是重新new一个
    //      override def reduce(buffer: Array[String], item: Array[String]): Array[String] = buffer.union(item).distinct
    //
    //      // 分区间聚合: 合并分区。依旧是用新值直接更新 buffer(初始值为zero)，并返回buffer本身，而不是重新new一个
    //      override def merge(b1: Array[String], b2: Array[String]): Array[String] = reduce(b1, b2)
    //
    //      // 最终结果汇总
    //      override def finish(reduction: Array[String]): Array[String] = reduction
    //
    //      // 定义内部缓存类型的编码器（前文提到的编码器，用于spark运算中的内部序列化和反序列化）
    //      override def bufferEncoder: Encoder[Array[String]] = newStringArrayEncoder
    //
    //      override def outputEncoder: Encoder[Array[String]] = bufferEncoder
    //    }

    spark.udf.register("combine_unique", udaf(new CombineUnique(newStringArrayEncoder)));

    spark.sql(
      """
        |select
        |    province,
        |    combine_unique(guidList) as newGuidList
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
