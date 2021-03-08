package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.{ArgsUtil, SparkUtil}
import org.apache.spark.sql.SparkSession

/**
 * App流量统计-会话聚合表计算：
 * 根据newsessionid分组，聚合
 * 求出每个回话的：起始时间，结束时间，访问页面数(pv)，入口页，跳出页
 */
object AppFlowAggSession {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1

    val spark = SparkUtil.getSparkSession(this.getClass.getSimpleName, true)

    val result = spark.sql(
      s"""
         |select guid,
         |       newsessionid as session_id,
         |       devicetype as device_type,
         |       newflag as isnew,
         |       location,
         |       start_time,
         |       end_time,
         |       osname as os_name,
         |       releasechannel as release_ch,
         |       if(size(hourNumSet) > 0, cast(array_min(hourNumSet) as int), null) as hour_range,
         |       if(size(pageList) > 0, pageList[0], null) as start_page,
         |       if(size(pageList) > 0, pageList[size(pageList)-1], null)  as end_page,
         |       size(pageList) as pv_cnt
         |from (
         |         select guid,
         |                newsessionid,
         |                newflag,
         |                location,
         |                devicetype,
         |                `timestamp`,
         |                osname,
         |                releasechannel,
         |                row_number() over (partition by newsessionid order by `timestamp` desc )                 as rn,
         |                first_value(`timestamp`) over (partition by newsessionid order by `timestamp`)           as start_time,
         |                last_value(`timestamp`) over (partition by newsessionid order by `timestamp`)            as end_time,
         |                collect_list(properties['pageId']) over (partition by newsessionid order by `timestamp`) as pageList,
         |                collect_set(from_unixtime(`timestamp` / 1000, 'yyyyMMddHH'))
         |                            over (partition by newsessionid order by `timestamp`)                        as hourNumSet
         |         from dwd.event_app_detail
         |         where dt = '${DT_CUR}'
         |     ) tmp
         |where rn = 1
         |
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.app_flow_agg_session partition(dt='${DT_CUR}')
         | select
         | guid, session_id,  start_time, end_time, start_page, end_page, pv_cnt,
         | isnew, hour_range, `location`, device_type, os_name, release_ch
         | from result
         |""".stripMargin)

    spark.close()
  }
}
