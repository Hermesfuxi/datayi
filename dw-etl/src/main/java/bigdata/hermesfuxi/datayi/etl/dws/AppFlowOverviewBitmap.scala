package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil.arrayToBitmap
import org.apache.spark.sql.SparkSession

object AppFlowOverviewBitmap {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val tmp = spark.sql(
      s"""
         |select
         |       cast(b.id as int) as id,
         |       a.`location`,
         |       a.hour_range,
         |       a.device_type,
         |       a.os_name,
         |       a.start_page,
         |       a.end_page,
         |       a.isnew,
         |       a.pv_cnt,      -- 用户的当日pv总数
         |       a.session_cnt, -- 用户的会话总数
         |       a.time_long,   -- 用户的总访问时长
         |       a.jumpout_cnt  -- 用户的跳出次数
         |
         |from (select * from dws.app_flow_agg_user where dt = '${DT_CUR}') a
         |         left join dwd.user_guid_global b
         |                   on a.guid = b.guid
        |
        |""".stripMargin)

//    tmp.show()
    tmp.createTempView("tmp")

    spark.udf.register("to_bitmap",arrayToBitmap)

    val result = spark.sql(
      s"""
         |SELECT location,
         |       hour_range,
         |       device_type,
         |       os_name,
         |       start_page,
         |       end_page,
         |       isnew,
         |       sum(pv_cnt)                                           as pv_cnt,
         |
         |       to_bitmap(collect_set(id))                            as uv_bitmap,
         |
         |       sum(session_cnt)                                      as session_cnt,
         |       sum(time_long)                                        as time_long,
         |
         |       to_bitmap(collect_set(if(session_cnt > 1, id, null))) as return_user_bitmap,
         |
         |       sum(jumpout_cnt)                                      as jumpout_cnt,
         |       to_bitmap(collect_set(if(jumpout_cnt > 0, id, null))) as jumpout_user_bitmap
         |
         |FROM tmp
         |GROUP BY location,
         |         hour_range,
         |         device_type,
         |         os_name,
         |         start_page,
         |         end_page,
         |         isnew
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert overwrite table dws.app_flow_overview_bitmap partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
