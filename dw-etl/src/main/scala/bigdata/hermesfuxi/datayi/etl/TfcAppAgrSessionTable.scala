package bigdata.hermesfuxi.datayi.etl

import org.apache.spark.sql.SparkSession

object TfcAppAgrSessionTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val result = spark.sql(
      s"""
         |select guid,
         |       newSessionId,
         |       devicetype,
         |       newFlag,
         |       location,
         |       startTime,
         |       endTime,
         |       hourNumSet,
         |       pageList
         |from (
         |         select guid,
         |                newSessionId,
         |                newFlag,
         |                location,
         |                devicetype,
         |                `timestamp`,
         |                row_number() over (partition by newsessionid order by `timestamp` desc )                 as rn,
         |                first_value(`timestamp`) over (partition by newsessionid order by `timestamp`)           as startTime,
         |                last_value(`timestamp`) over (partition by newsessionid order by `timestamp`)            as endTime,
         |                collect_list(properties['pageId']) over (partition by newsessionid order by `timestamp`) as pageList,
         |                collect_set(from_unixtime(`timestamp` / 1000, 'yyyyMMddHH'))
         |                            over (partition by newsessionid order by `timestamp`)                        as hourNumSet
         |         from dwd.event_app_log_to_dwd
         |         where dt = '${args(0)}'
         |     ) tmp
         |where rn = 1
         |""".stripMargin)

    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.tfc_app_agr_session partition(dt='${args(0)}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
