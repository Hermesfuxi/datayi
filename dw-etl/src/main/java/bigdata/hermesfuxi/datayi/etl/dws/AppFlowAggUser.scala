package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * App流量统计-用户聚合表计算：
 * 根据newsessionid分组，聚合
 * 求出每个回话的：起始时间，结束时间，访问页面数(pv)，入口页，跳出页
 */
object AppFlowAggUser {
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
         |SELECT
         |  guid         ,
         |  `location`   ,
         |  hour_range   ,
         |  device_type  ,
         |  os_name      ,
         |  start_page   ,
         |  end_page     ,
         |  isnew        ,
         |  count(1)                    as  session_cnt  ,  -- 会话次数
         |  sum(cast(end_time as bigint) - cast(start_time as bigint))    as  time_long    ,  -- 会话时长
         |  count(if(pv_cnt<2,1,null))  as  jumpout_cnt  ,  -- 跳出次数
         |  sum(pv_cnt)                 as  pv_cnt          -- 总访问页数
         |FROM dws.app_flow_agg_session
         |WHERE dt='${DT_CUR}'
         |GROUP BY
         |  guid         ,
         |  `location`   ,
         |  hour_range   ,
         |  device_type  ,
         |  os_name       ,
         |  start_page   ,
         |  end_page     ,
         |  isnew
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.app_flow_agg_user partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
