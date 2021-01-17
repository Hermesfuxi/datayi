package bigdata.hermesfuxi.datayi.etl

import bigdata.hermesfuxi.datayi.etl.utils.DateUtils
import org.apache.spark.sql.SparkSession

object AppGuidTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val DT_PRE = DateUtils.getPlusFormatDate(-1, args(0), "yyyy-MM-dd");
    spark.sql("select  max(id), count(distinct guid) from dwd.user_guid_global").show()

//    val result = spark.sql(
//      s"""
//         |SELECT row_number() over (ORDER BY a.guid) as id,
//         |       a.guid
//         |FROM (
//         |         SELECT
//         |         distinct nvl(account, deviceid) as guid
//         |         FROM dwd.device_account_relation
//         |         WHERE dt = '${args(0)}'
//         |         GROUP BY nvl(account, deviceid)
//         |     ) a
//         |
//         |         LEFT JOIN
//         |     (
//         |         SELECT guid
//         |         FROM dwd.user_guid_global
//         |         where dt = '${DT_PRE}'
//         |         group by guid
//         |     ) b
//         |     ON a.guid = b.guid
//         |WHERE b.guid is null
//         |
//         |""".stripMargin)
//
//    result.createTempView("result")
//    spark.sql(
//      s"""
//         | insert into table dwd.user_guid_global partition(dt='${args(0)}')
//         | select * from result
//         |""".stripMargin)

    spark.close()
  }
}
