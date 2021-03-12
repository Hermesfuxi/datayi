import bigdata.hermesfuxi.datayi.functions.BitmapOrAggregation
import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf

/**
 * 使用hll_init_agg函数可创建中间结果HLL sketch。其是一个二进制数组结构，因此可建如下测试表：
 drop table if exists test.hllp_alchemy;
CREATE TABLE if not exists test.hllp_alchemy
(
    id_mod bigint,
    hll_id binary
)STORED AS PARQUET;
 */
object HLLTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.range(100000).toDF("id").createOrReplaceTempView("ids")

    // Register spark-alchemy HLL functions for use from SparkSQL
    com.swoop.alchemy.spark.expressions.hll.HLLFunctionRegistration.registerFunctions(spark)
//    hll_init
//    hll_init_collection
//    hll_init_agg
//    hll_init_collection_agg
//    hll_merge
//    hll_row_merge
//    hll_cardinality
//    hll_intersect_cardinality
//    hll_convert

    spark.udf.register("to_bitmap", RoaringBitmapUtil.arrayToBitmap)
    spark.udf.register("bitmap_card", RoaringBitmapUtil.getCardNum _)
    spark.udf.register("bitmap_card_by_int", RoaringBitmapUtil.getCardNumByInt _)

    spark.sql(
      """
        | select
        |    -- exact distinct count
        |    count(distinct id) as cntd,
        |
        |    -- roaringBitmap distinct count
        |    bitmap_card_by_int(collect_set(id)) as bitmap_count,
        |    bitmap_card(to_bitmap(collect_set(id))) as bitmap_count2,
        |
        |    -- Spark's HLL implementation with default 5% precision
        |    approx_count_distinct(id) as anctd_spark_default,
        |
        |    -- approximate distinct count with default 5% precision
        |    hll_cardinality(hll_init_agg(id)) as acntd_default,
        |
        |    -- approximate distinct counts with custom precision
        |    map(0.005, hll_cardinality(hll_init_agg(id, 0.005))) as acntd1,
        |    map(0.010, hll_cardinality(hll_init_agg(id, 0.010))) as acntd2,
        |    map(0.020, hll_cardinality(hll_init_agg(id, 0.020))) as acntd3,
        |    map(0.050, hll_cardinality(hll_init_agg(id, 0.050))) as acntd4,
        |    map(0.100, hll_cardinality(hll_init_agg(id, 0.100))) as acntd5
        |
        |from ids
        |""".stripMargin).show(10, false)
    /*
+------+------------+-------------+-------------------+-------------+----------------+----------------+----------------+----------------+-----------------+
|cntd  |bitmap_count|bitmap_count2|anctd_spark_default|acntd_default|acntd1          |acntd2          |acntd3          |acntd4          |acntd5           |
+------+------------+-------------+-------------------+-------------+----------------+----------------+----------------+----------------+-----------------+
|100000|100000      |100000       |95546              |98566        |[0.005 -> 99593]|[0.010 -> 98093]|[0.020 -> 98859]|[0.050 -> 98566]|[0.100 -> 106476]|
+------+------------+-------------+-------------------+-------------+----------------+----------------+----------------+----------------+-----------------+
     */

    // 通过SparkSQL的方式写入数据
//    spark.sql(
//      """
//        |insert overwrite table test.hllp_alchemy
//        |select
//        |       id % 10          as id_mod,
//        |       hll_init_agg(id) as hll_id
//        |from ids
//        |group by id % 10
//        |""".stripMargin)

    //读表数据，并计算其基数值
    spark.sql(
      """
        |select
        |       id_mod,
        |       hll_cardinality(hll_id) as acntd
        |from test.hllp_alchemy
        |order by id_mod
        |""".stripMargin).show()
/*
+------+-----+
|id_mod|acntd|
+------+-----+
|     0| 9554|
|     1| 9884|
|     2| 9989|
|     3|10159|
|     4| 9987|
|     5| 9770|
|     6| 9921|
|     7| 9774|
|     8| 9414|
|     9| 9390|
+------+-----+
 */

    // 读表数据，并进行再聚合计算
    spark.sql(
      """
        |select
        |       id_mod % 2 as id_mod2,
        |       hll_cardinality(hll_merge(hll_id)) as acntd
        |from test.hllp_alchemy
        |group by id_mod % 2
        |""".stripMargin).show()
    /*
  +-------+-----+
|id_mod2|acntd|
+-------+-----+
|      0|47305|
|      1|53156|
+-------+-----+
     */

    // 如不想使用二进制的列，也可将二进制数据转为十六进制的字符串进行存储
    //    spark.sql(
    //      """
    //        |insert overwrite table test.hllp_alchemy
    //        |select
    //        |       id % 10 as id_mod10,
    //        |       hex(hll_init_agg(id)) as hll_id
    //        |from ids
    //        |group by id % 10
    //        |""".stripMargin)

    // 从Hive表中读出数据并恢复成二进制数组数据进行计算
//    spark.sql(
//      """
//        |select
//        |       id_mod,
//        |       hll_cardinality(unhex(hll_id)) as acntd
//        |from test.hllp_alchemy
//        |order by id_mod
//        |""".stripMargin).show()

    spark.close()
  }

}
