package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.functions.{BitmapOrAggregation, BitmapOrAggregationFunction}
import bigdata.hermesfuxi.datayi.utils.{ArgsUtil, RoaringBitmapUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf

object AppFlowOverviewBitmapCube {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      //该表字段个数比较多，超过了默认值25，需要另行设置
      .config("spark.debug.maxToStringFields", "100")
      .getOrCreate()

    spark.udf.register("bitmap_or_agg", udaf(BitmapOrAggregation))
    spark.udf.register("bitmap_card", RoaringBitmapUtil.getCardNum _)

    val result = spark.sql(
      s"""
         |SELECT location,
         |       hour_range,
         |       device_type,
         |       os_name,
         |       start_page,
         |       end_page,
         |       isnew,
         |       sum(pv_cnt)                                     as pv_cnt,
         |       bitmap_card(bitmap_or_agg(uv_bitmap))           as uv_cnt,
         |       sum(session_cnt)                                as session_cnt,
         |       sum(time_long)                                  as time_long,
         |       bitmap_card(bitmap_or_agg(return_user_bitmap))  as return_user_cnt,
         |       sum(jumpout_cnt)                                as jumpout_cnt,
         |       bitmap_card(bitmap_or_agg(jumpout_user_bitmap)) as jumpout_user_cnt
         |
         |FROM dws.app_flow_overview_bitmap
         |where dt='${DT_CUR}'
         |GROUP BY location,
         |         hour_range,
         |         device_type,
         |         os_name,
         |         start_page,
         |         end_page,
         |         isnew
         |with cube
         |""".stripMargin)

//    result.show()  // 有许多null
    result.createTempView("result")
    // 维度报表结果查询示例，比如，查询按手机型号统计的各指标
//    spark.sql(
//      """
//        |    SELECT  *  FROM result
//        |        where coalesce(
//        |          `location`   ,
//        |          hour_range   ,
//        |          os_name      ,
//        |          start_page   ,
//        |          end_page     ,
//        |          isnew
//        |        ) is null and device_type is not null
//        |""".stripMargin).show()

    // 插入表中
    spark.sql(
      s"""
         | insert overwrite table dws.app_flow_overview_bitmap_cube partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
