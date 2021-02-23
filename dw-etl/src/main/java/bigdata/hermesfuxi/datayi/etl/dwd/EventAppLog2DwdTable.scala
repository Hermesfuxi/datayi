package bigdata.hermesfuxi.datayi.etl.dwd

import java.io.InputStreamReader

import bigdata.hermesfuxi.datayi.beans.AppLogBean
import bigdata.hermesfuxi.datayi.utils.{ArgsUtil, HDFSUtil}
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object EventAppLog2DwdTable {
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

    val sparkContext = spark.sparkContext
    /**
     * 加载各种字典数据，并广播
     */
    // 1.geohash字典
    val geoHash = spark.read.parquet("hdfs://hadoop-master:9000/datayi/dicts/geodict/")
    val geoHashMap: collection.Map[String, String] = geoHash.rdd.map(row => {
      val geohash = row.getAs[String]("geohash")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val region = row.getAs[String]("region")
      (geohash, s"${province}|${city}|${region}")
    }).collectAsMap()
    val geoHashMapRef = sparkContext.broadcast(geoHashMap)

    // 2.ip2region.db字典
//    方式1: 利用 addFile/SparkFiles.get 广播文件和文件夹
//    val path = this.getClass.getResource("/db/ip2region.db").getPath
//    sparkContext.addFile(path)

    // 自己读文件，存入一个字节数组，并广播
    val fs = FileSystem.get(new Configuration())
    val path = new Path("/datayi/dicts/ip2region.db")
    // 获取文件的长度（字节数）
    val statuses: Array[FileStatus] = fs.listStatus(path)
    val length = statuses(0).getLen

    // 将字典文件，以字节形式读取并缓存到一个字节buffer中
    val in: FSDataInputStream = fs.open(path)
    val buffer = new Array[Byte](length.toInt)
    in.readFully(0, buffer)
    val ip2regionRef = spark.sparkContext.broadcast(buffer)


    // 3.设备账号关联评分字典: 用于推断游客的登录账号
    val relationDic = spark.sql(
      s"""
         |select deviceid,
         |       account
         |from (
         |         select deviceid,
         |                account,
         |                row_number() over (partition by deviceid order by score desc,last_time desc) as rn
         |         from dwd.device_account_relation
         |         where dt = '${DT_CUR}'
         |     ) o
         |where rn = 1
         |""".stripMargin)
    val relationDicMap = relationDic.rdd.map(row => {
      val deviceid = row.getAs[String]("deviceid")
      val account = row.getAs[String]("account")
      (deviceid, account)
    }).collectAsMap()
    val relationDicMapRef = sparkContext.broadcast(relationDicMap)


    // 4.全局guid表(from整个表）
    val guidList: Array[String] = spark.read.table("dwd.user_guid_global")
      .select("guid")
      .rdd.map(_.getAs[String]("guid"))
      .collect()
    val guidListRef = spark.sparkContext.broadcast(guidList.toSet)

    /**
     * 加载T日的ODS日志表数据
     */
    val logTable = spark.read.table("ods.event_app_log").where(s"dt='${DT_CUR}'")

    /**
     * 根据规则清洗过滤
     * 1，去除json数据体中的废弃字段（前端开发人员在埋点设计方案变更后遗留的无用字段）：
     * 2，过滤掉json格式不正确的（脏数据）
     * 3，过滤掉日志中 account 及 deviceid 全为空的记录
     * 4，过滤掉日志中缺少关键字段（properties/eventid/sessionid 缺任何一个都不行）的记录！
     */
    val filteredLog = logTable
      .where("account is not null")
      .where("account != ''")
      .where("deviceid is not null")
      .where("deviceid != ''")
      .where("properties is not null")
      .where("eventid is not null")
      .where("eventid != ''")
      .where("sessionid is not null")
      .where("sessionid != ''")

    filteredLog.createTempView("log")

    /**
     * session分割，添加新的newsessionid字段
     */
    val tableColumns = "account,appid,appversion,carrier,deviceid,devicetype,eventid,ip,latitude,longitude,nettype,osname,osversion,properties,releasechannel,resolution,sessionid,`timestamp`"

    val newSessionIdTable = spark.sql(
      s"""
         | select
         |   ${tableColumns},
         |   concat_ws("_", sessionid, sum(time_diff) over (partition by sessionid order by `timestamp`)) newSessionId
         | from (
         |   select
         |       ${tableColumns},
         |       lag(`timestamp`, 1, `timestamp`) over (partition by sessionid order by `timestamp`) lag_time,
         |       if((`timestamp` - lag(`timestamp`, 1, `timestamp`) over (partition by sessionid order by `timestamp`)) > 30 * 60 * 1000, 1, 0) time_diff
         |   from log
         | ) e
         |""".stripMargin)

    /**
     * 集成数据（地理位置）
     */
    import spark.implicits._
    val regionResult = newSessionIdTable.rdd.mapPartitions(iter => {
      val geoHashMap = geoHashMapRef.value

      val ipRegionBitArray: Array[Byte] = ip2regionRef.value
      val searcher = new DbSearcher(new DbConfig(), ipRegionBitArray)

      iter.map(row => {
        // 地理位置: 先 gps, 再 ip
        // gps 定位
        val bean = AppLogBean.row2AppLogBean(row)
        val geohash = GeoHash.geoHashStringWithCharacterPrecision(bean.latitude, bean.longitude, 5)
        var location = geoHashMap.getOrElse(geohash, "")

        // ip 定位
        if (StringUtils.isEmpty(location)) {
          val ip = row.getAs[String]("ip")
          val block = searcher.memorySearch(ip)
          // 中国|0|上海|上海市|电信
          val regionArr = block.getRegion.split("[|]")
          val len = regionArr.length
          for (i <- 0 until len) {
            if (i != 1 && len >= 4 && i < 4) {
              location = location + s"|${regionArr(i)}"
            }
          }
        }
        bean.location = location
        bean
      })
    })

    /**
     * 集成数据（guid全局用户标识，新老访客标识）
     */

    val result = regionResult.mapPartitions(iter => {

      val relationDicMap = relationDicMapRef.value
      val guidList = guidListRef.value

      iter.map(bean => {
        // guid全局用户标识
        var guid = ""
        // 如果该条数据中，有登录账号，则直接用该登录账号作为这条数据的全局用户标识
        if (StringUtils.isNotBlank(bean.account)) {
          guid = bean.account
        } else {
          // 如果该条数据中，没有登录账号，则用设备id去关联账号表中查找默认的账号，作为guid
          // 如果查询到的结果为不为null，则用查询到的account作为guid，否则用deviceid作为guid
          guid = relationDicMap.getOrElse(bean.deviceid, bean.deviceid)
        }
        bean.guid = guid

        // 新老访客
        var newflag = false
        if (!guidList.contains(guid)) {
          newflag = true
        }
        bean.newflag = newflag
        bean
      })
    }).toDF()

    /**
     * 保存结果到目标表
     */
//        result.show()
    result.createTempView("result")

    /**
     * 将 result 插入 dwd.event_app_detail 表
     */
    spark.sql(
      s"""
         | insert into table dwd.event_app_detail partition(dt='${DT_CUR}')
         | select ${tableColumns}, newsessionid, location, guid, newflag from result
         |""".stripMargin)

    /**
     * 将 新访客GUID 插入 dwd.guid表
     */
    spark.sql(
      s"""
         |with a as (select nvl(max(id), 0) as max_id from dwd.user_guid_global),
         |     b as (
         |         select guid
         |         from result
         |         where newflag = true
         |         group by guid
         |     )
         |insert into table dwd.user_guid_global partition (dt = '${DT_CUR}')
         |select (row_number() over (order by b.guid) + a.max_id) as id,
         |       b.guid
         |from b join a
         |""".stripMargin)

    /**
     * 将 活跃GUID 插入 dws.每日活跃用户记录表
     */
    spark.sql(
      s"""
         |insert into table dws.user_active_day partition (dt = '${DT_CUR}')
         |select guid
         |from result
         |group by guid
         |""".stripMargin)

    spark.close()
  }
}
