package bigdata.hermesfuxi.datayi.etl

import bigdata.hermesfuxi.datayi.etl.utils.DateUtils
import org.apache.spark.sql.SparkSession

/**
 *  基于T-1日的"关联得分表" 和 T日的行为日志，得出T日的"关联得分表"
 */
object DeviceAccountRelationScore {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.shuffle.partitions","10")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // 加载T-1日的设备账号关联评分表
    val relation = session.read.table("dwd.device_account_relation").where(s"dt='${DateUtils.getPlusFormatDate(-1, args(0), "yyyy-MM-dd")}'")
    // 加载T日的日志表
    val log = session.read.table("ods.event_app_log").where(s"dt='${args(0)}'")

    relation.createTempView("relation")

    log.createTempView("log_detail")

    /**
     * 先聚合计算T日的日志中的设备&账号评分
     */
    val logGroup = session.sql(
      """
        |    select
        |       deviceid,
        |       if(trim(account)='',null,account) as account,
        |       cast(count(distinct sessionid)*100 as double) as score,
        |       min(timestamp)  as time
        |    from log_detail
        |    group by deviceid,account
        |""".stripMargin)

    logGroup.createTempView("log")

    val frame = session.sql(
      """
        |select
        |  nvl(re.deviceid,log.deviceid) as deviceid,
        |  nvl(re.account,log.account) as account,
        |  if(re.deviceid is not null and log.deviceid is null,re.score*0.7,nvl(re.score,0)+log.score)  score,
        |  nvl(re.first_time,log.time)  first_time,
        |  nvl(log.time,re.last_time) as last_time
        |
        |from relation re full join log
        |on re.deviceid=log.deviceid and re.account=log.account
        |""".stripMargin)

    // 从这个结果中，去掉那些已经有了关联账号的空设备
    frame.where("account is null").createTempView("t_null")
    frame.where("account is not null").createTempView("t_not_null")

    val result = session.sql(
      """
        |select
        |  nvl(t_not_null.deviceid,t_null.deviceid) as deviceid,
        |  t_not_null.account as account,
        |  t_not_null.score as score,
        |  t_not_null.first_time as first_time,
        |  t_not_null.last_time as last_time
        |
        |from t_null full join t_not_null on t_null.deviceid=t_not_null.deviceid
        |
        |""".stripMargin)

    result.createTempView("result")
    session.sql(s"insert into table dwd.device_account_relation partition(dt='${args(0)}') select * from result")

    session.close()
  }
}
