package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * 用户留存分析表计算中间表: 从拉链表 dws.user_active_time_range 中查询实现
 */
object UserRetention {
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
         |SELECT
         |       '${DT_CUR}'                     AS calc_dt,
         |       first_dt,
         |       datediff('${DT_CUR}', first_dt) as retention_days,
         |       count(1)                        as retention_users
         |FROM dws.user_active_time_range
         |
         |WHERE dt = '${DT_CUR}'
         |  AND datediff('${DT_CUR}', first_dt) <= 30
         |  AND range_end = '9999-12-31'
         |
         |GROUP BY datediff('${DT_CUR}', first_dt), first_dt
         |""".stripMargin)

    result.show()
//    result.createTempView("result")
//    spark.sql(
//      s"""
//         | insert into table dws.user_retention partition(dt='${DT_CUR}')
//         | select * from result
//         |""".stripMargin)

    spark.close()
  }
}
