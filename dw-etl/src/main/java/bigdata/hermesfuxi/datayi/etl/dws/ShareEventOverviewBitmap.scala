package bigdata.hermesfuxi.datayi.etl.dws

import java.util.Properties

import bigdata.hermesfuxi.datayi.functions.BitmapOrAggregation
import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil.arrayToBitmap
import bigdata.hermesfuxi.datayi.utils.{ArgsUtil, RoaringBitmapUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udaf

object ShareEventOverviewBitmap {
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

    // 加载分享事件明细数据
    val shareEvents = spark.read.table("dwd.event_app_detail")
      .where(s"dt='${DT_CUR}' and eventid='share'")
      // 列裁剪
      .select("guid","devicetype","timestamp","properties")

    // 从mysql中加载维表
    val props = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("mysql.properties"))
    val url = props.getProperty("url")
    // 商品信息维表
    val productInfo = spark.read.jdbc(url,"dim_product_info",props)

    // 页面信息维表
    val pageInfo = spark.read.jdbc(url,"dim_page_info",props)

    shareEvents.createTempView("events")
    productInfo.createTempView("productinfo")
    pageInfo.createTempView("pageinfo")


    // 星型模型思想
    // 星型模型，是一种数仓中的建模思想（表结构设计）
    // 它的特点是，提炼出 “事实表”，并提炼出相关分析需要的维度表，那么，这个事实表+这些维度表，就是一个模型（星型模型）
    // 在具体分析运算时：拿着中心表（事实表） 关联各种  维度表  ，形成一张大宽表之后计算
    val joined = spark.sql(
      """
        |
        |select
        |       cast(d.id as int)                                        as id,
        |       a.devicetype                                             as device_type,
        |       hour(from_unixtime(cast(a.timestamp / 1000 as bigint)))  as hour_range,
        |       nvl(b.cat_name, '未知')                                   as cat_name,
        |       nvl(b.brand_name, '未知')                                 as brand_name,
        |       a.properties['pageId']                                   as page_id,
        |       nvl(c.lanmu_name, '未知')                                 as lanmu_name,
        |       a.properties['shareMethod']                              as share_method
        |
        |from events a
        |         left join productinfo b on a.properties['productId'] = b.id
        |         left join pageinfo c on a.properties['pageId'] = c.id
        |         join dwd.user_guid_global d on a.guid = d.guid
        |
        |""".stripMargin)


    joined.createTempView("joined")

    spark.udf.register("to_bitmap",arrayToBitmap)

    val result = spark.sql(
      s"""
         |
         |SELECT
         |   cat_name,
         |   brand_name,
         |   page_id,
         |   lanmu_name,
         |   share_method,
         |   hour_range,
         |   device_type,
         |   to_bitmap(collect_set(id)) as guid_bitmap,
         |   count(1) as share_cnt
         |
         |FROM joined
         |GROUP BY
         |   cat_name,
         |   brand_name,
         |   page_id,
         |   lanmu_name,
         |   share_method,
         |   hour_range,
         |   device_type
         |
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.share_event_overview_bitmap partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
