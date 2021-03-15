package attempt.agg

import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil
import org.apache.spark.sql.SparkSession

/**
 * 使用hll_init_agg函数可创建中间结果HLL sketch。其是一个二进制数组结构，因此可建如下测试表：
 * drop table if exists test.hllp_alchemy;
 * CREATE TABLE if not exists test.hllp_alchemy
 * (
 * id_mod bigint,
 * hll_id binary
 * )STORED AS PARQUET;
 */
object HLLTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    //    import com.swoop.alchemy.spark.expressions.hll.Implementation._
    // 使用的基础HLL实现算法（AGKN/ AGGREGATE_KNOWLEDGE/ STRM/STREAM_LIB (默认)）
    //    spark.conf.set("com.swoop.alchemy.hll.implementation", "AGGREGATE_KNOWLEDGE")

    //    hll_init(column, relativeSD, implementation)：将字段的值（类型为普通类型）转为 HLL略图（草图）
    //    hll_init_collection(array_or_map_column, relativeSD, implementation)：将字段的值（类型为数组、集合、Map等）转为 HLL略图（草图）,无需爆炸和重新分组数据
    //    hll_init_agg(column, relativeSD, implementation)：将groupBy时聚合的普通字段值列表转为 HLL略图，逻辑 = hll_merge(hll_init(...))
    //    hll_init_collection_agg(array_or_map_column, relativeSD, implementation)：将groupBy时聚合的集合字段值列表，转为 HLL略图，逻辑 = hll_merge(hll_init_collection(...))
    //    hll_merge(hll_sketch, implementation)：聚合合并HLL草图
    //    hll_row_merge(implementation, hll_sketches*)：将一行中的多个草图合并到一个字段中
    //    hll_cardinality(hll_sketch, implementation)：计算基数
    //    hll_intersect_cardinality(hll_sketch, hll_sketch, implementation)：计算交集基数
    //    hll_convert(hll_sketch, from, to)：在hll_sketch类型之间转换, 当前唯一支持的转换: from STREAM_LIB to AGGREGATE_KNOWLEDGE,可用于将现有StreamLib草图加载到Postgres中。
    //    注意：转换后的草图不应与相同类型的未转换草图合并，否则将导致对转换后的草图和“本机”草图中存在的所有值进行重复计数

    // DSL 风格
    import RoaringBitmapUtil._
    import com.swoop.alchemy.spark.expressions.hll.functions._
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val bitmap_card = udf(getCardNum _)
    val bitmap_card_by_int = udf(getCardNumByInt _)
    val to_bitmap = udf(arrayToBitmap _)

    val ids = spark.range(100000)
    ids.select(
      // exact distinct count
      countDistinct('id).as("cntd"),

      // roaringBitmap
      bitmap_card_by_int(collect_set('id)).as("bitmap_count"),
      bitmap_card(to_bitmap(collect_set('id))).as("bitmap_count2"),

      // Spark's HLL implementation with default 5% precision
      approx_count_distinct('id).as("anctd_spark_default"),

      // approximate distinct count with default 5% precision
      hll_cardinality(hll_init_agg('id)).as("acntd_default"),

      // approximate distinct counts with custom precision
      map(
        Seq(0.005, 0.010, 0.020, 0.050, 0.100).flatMap { error =>
          lit(error) :: hll_cardinality(hll_init_agg('id, error)) :: Nil
        }: _*
      ).as("acntd")
    ).show(10, false)

    // SQL 风格
    spark.range(100000).toDF("id").createOrReplaceTempView("ids")
    // Register spark-alchemy HLL functions for use from SparkSQL
    com.swoop.alchemy.spark.expressions.hll.HLLFunctionRegistration.registerFunctions(spark)
    spark.udf.register("to_bitmap", RoaringBitmapUtil.arrayToBitmap)
    spark.udf.register("bitmap_card", RoaringBitmapUtil.getCardNum _)
    spark.udf.register("bitmap_card_by_int", RoaringBitmapUtil.getCardNumByInt _)
    spark.sql(
      """
        | select
        |    -- exact distinct count
        |    count(distinct id) as cntd,
        |
        |    -- roaringBitmap
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

    // 写入数据测试数据
    //    ids.groupBy('id % 10)
    //      .agg(hll_init_agg('id).as("hll_id"))
    //      .select(
    //        ('id % 10).as("id_mod"),
    //        ('hll_id).as("hll_id"),
    //      ).writeTo("test.hllp_alchemy")

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


    // 如不想使用二进制的列，也可将二进制数据转为十六进制的字符串（hex），再进行存储
    //    spark.sql(
    //      """
    //        |insert overwrite table test.hllp_alchemy
    //        |select
    //        |       id % 10 as id_mod10,
    //        |       hex(hll_init_agg(id)) as hll_id
    //        |from ids
    //        |group by id % 10
    //        |""".stripMargin)

    // 从Hive表中读出数据,并将十六进制恢复成二进制数组数据（unhex），再进行计算
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
