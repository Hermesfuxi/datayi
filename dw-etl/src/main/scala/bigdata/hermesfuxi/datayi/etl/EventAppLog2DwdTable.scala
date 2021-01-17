package bigdata.hermesfuxi.datayi.etl

import bigdata.hermesfuxi.datayi.etl.beans.AppLogBean
import bigdata.hermesfuxi.datayi.etl.utils.DateUtils
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

object EventAppLog2DwdTable {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ods层app端行为日志数据，处理为dwd明细表")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val sparkContext = spark.sparkContext
    /**
     * 加载各种字典数据，并广播
     */
    // 1.geohash字典
    val geoHash = spark.read.parquet("hdfs://hadoop-master:9000/dicts/geodict/")
    val geoHashMap: collection.Map[String, String] = geoHash.rdd.map(row => {
      val geohash = row.getAs[String]("geohash")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val region = row.getAs[String]("region")
      (geohash, s"${province}|${city}|${region}")
    }).collectAsMap()
    val geoHashMapRef = sparkContext.broadcast(geoHashMap)

    // 2.ip2region.db字典
    val path = this.getClass.getResource("/db/ip2region.db").getPath
    sparkContext.addFile(path)

    /**
     * 加载T日的ODS日志表数据
     */
    val log = spark.read.table("ods.event_app_log").where(s"dt='${args(0)}'")

    /**
     * 根据规则清洗过滤
     * 1，去除json数据体中的废弃字段（前端开发人员在埋点设计方案变更后遗留的无用字段）：
     * 2，过滤掉json格式不正确的（脏数据）
     * 3，过滤掉日志中 account 及 deviceid 全为空的记录
     * 4，过滤掉日志中缺少关键字段（properties/eventid/sessionid 缺任何一个都不行）的记录！
     */
    val filteredLog = log
      .where("account is not null")
      .where("account != ''")
      .where("deviceid is not null")
      .where("deviceid != ''")
      .where("properties is not null")
      .where("eventid is not null")
      .where("eventid != ''")
      .where("sessionid is not null")
      .where("sessionid != ''")

    // 3.设备账号关联评分字典: 用于推断游客的登录账号
    val relationDic = spark.sql(
      s"""
        |
        |select
        |    deviceid,
        |    account
        |   from
        |      (
        |         select
        |          deviceid,
        |          account,
        |          row_number() over(partition by deviceid order by score desc,last_time desc) as rn
        |         from dwd.device_account_relation
        |         where dt='${args(0)}'
        |      ) o
        |where rn=1
        |
        |""".stripMargin)

    val relationDicMap = relationDic.rdd.map(row => {
      val deviceid = row.getAs[String]("deviceid")
      val account = row.getAs[String]("account")
      (deviceid, account)
    }).collectAsMap()
    val relationDicMapRef = sparkContext.broadcast(relationDicMap)

    val allOldDevAccSet = spark.read.table("dwd.device_account_relation")
      .where(s"dt='${DateUtils.getPlusFormatDate(-1, args(0), "yyyy-MM-dd")}'")
      .selectExpr("explode (array(deviceid,account)) as allOldDevAcc").rdd
      .map(row=>row.getAs[String]("allOldDevAcc")).collect().toSet
    val allOldDevAccSetRef = sparkContext.broadcast(allOldDevAccSet)

    /**
     * session分割，添加新的newsessionid字段
     */
    filteredLog.createTempView("log")

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
     * 集成数据（地理位置，guid全局用户标识，新老访客标识）
     */
    import spark.implicits._
    val result = newSessionIdTable.rdd.mapPartitions(iter => {
      val geoHashMap = geoHashMapRef.value

      val pathStr = SparkFiles.get("ip2region.db")
      val searcher = new DbSearcher(new DbConfig(), pathStr)

      val relationDicMap = relationDicMapRef.value

      val allOldDevAccSet = allOldDevAccSetRef.value

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

        // guid全局用户标识
        var guid = ""
        // 如果该条数据中，有登录账号，则直接用该登录账号作为这条数据的全局用户标识
        val account = row.getAs[String]("account")
        val deviceid = row.getAs[String]("deviceid")
        if (StringUtils.isNotBlank(account)) {
          guid = account
        } else {
          // 如果该条数据中，没有登录账号，则用设备id去关联账号表中查找默认的账号，作为guid
          // 如果查询到的结果为不为null，则用查询到的account作为guid，否则用deviceid作为guid
          guid = relationDicMap.getOrElse(deviceid, deviceid)
        }
        bean.guid = guid

        // 新老访客
        var newFlag = false
        if (!allOldDevAccSet.contains(account) && !allOldDevAccSet.contains(deviceid)) {
          newFlag = true
        }
        bean.newFlag = newFlag
        bean
      })
    }).toDF()

    /**
     * 保存结果到目标表
     */
//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dwd.event_app_log_to_dwd partition(dt='${args(0)}')
         | select ${tableColumns}, newSessionId, location, guid, newFlag from result
         |""".stripMargin)
    spark.close()

  }
}
