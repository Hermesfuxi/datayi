import org.apache.spark.sql.SparkSession

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

    // 读表数据，并进行再聚合计算
    spark.sql(
      """
        |select
        |       id_mod % 2 as id_mod2,
        |       hll_cardinality(hll_merge(hll_id)) as acntd
        |from test.hllp_alchemy
        |group by id_mod % 2
        |""".stripMargin).show()

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
