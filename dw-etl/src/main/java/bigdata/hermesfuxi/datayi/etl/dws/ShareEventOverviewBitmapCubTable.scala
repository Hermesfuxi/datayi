package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

object ShareEventOverviewBitmapCubTable {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
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
         |         from dwd.event_app_detail
         |         where dt = '${DT_CUR}'
         |     ) tmp
         |where rn = 1
         |""".stripMargin)

    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.app_flow_agg_user partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
