package bigdata.hermesfuxi.datayi.etl.ads

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * 连续活跃天数分布报表：从 拉链表 dws.user_active_time_range 中查询实现
 */
object UserActiveContinuousDistribute {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1
    val DT_PRE = DT._2

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val result = spark.sql(
      s"""
         |
         |
         |""".stripMargin)

    result.show()
        result.createTempView("result")
        spark.sql(
          s"""
             | insert into table dws.user_retention partition(dt='${DT_CUR}')
             | select * from result
             |""".stripMargin)

    spark.close()
  }
}
