package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.functions.BitmapOrAggregation
import bigdata.hermesfuxi.datayi.utils.{ArgsUtil, RoaringBitmapUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf

object ShareEventOverviewBitmapCub {
  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1


    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("bitmap_or_agg", udaf(BitmapOrAggregation))
    spark.udf.register("bitmap_card", RoaringBitmapUtil.getCardNum _)

    val result = spark.sql(
      s"""
         |select cat_name,
         |       brand_name,
         |       page_id,
         |       lanmu_name,
         |       share_method,
         |       hour_range,
         |       device_type,
         |       bitmap_card(bitmap_or_agg(guid_bitmap)) as user_cnt,
         |       sum(share_cnt)              as share_cnt
         |
         |from dws.share_event_overview_bitmap
         |where dt = '${DT_CUR}'
         |group by cat_name,
         |         brand_name,
         |         page_id,
         |         lanmu_name,
         |         share_method,
         |         hour_range,
         |         device_type
         |with cube
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.share_event_overview_bitmap_cube partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
