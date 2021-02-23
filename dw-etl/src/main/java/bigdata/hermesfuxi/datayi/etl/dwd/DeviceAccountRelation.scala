package bigdata.hermesfuxi.datayi.etl.dwd

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * 基于T-1日的"关联得分表" 和 T日的行为日志，得出T日的"关联得分表"
 */
object DeviceAccountRelation {

  def main(args: Array[String]): Unit = {
    // 默认是 T 为 昨天, T-1 为 前天
    val DT = ArgsUtil.initArgs(args)
    val DT_CUR = DT._1
    val DT_PRE = DT._2

    val session = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.shuffle.partitions", "10")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // 加载T-1日的设备账号关联评分表
    val pre_relation = session.read.table("dwd.device_account_relation").where(s"dt='${DT_PRE}'")
    pre_relation.createTempView("pre_relation")

    // 加载T日的日志表
    val cur_log = session.read.table("ods.event_app_log").where(s"dt='${DT_CUR}'")
    cur_log.createTempView("cur_log")

    /**
     * 先聚合计算T日的日志中的设备&账号评分
     */
    val cur_log_group = session.sql(
      """
        |select
        |       deviceid,
        |       if(trim(account) = '', null, account)           as account,
        |       cast(count(distinct sessionid) * 100 as double) as score,
        |       min(`timestamp`)                                  as min_time
        |from cur_log
        |group by deviceid, account
        |
        |""".stripMargin)

    cur_log_group.createTempView("cur_log_group")

    val frame = session.sql(
      """
        |select
        |       nvl(re.deviceid, log.deviceid) as deviceid,
        |       nvl(re.account, log.account)   as account,
        |       if(re.deviceid is not null and log.deviceid is null,
        |           re.score * 0.7,
        |           nvl(re.score, 0) + log.score
        |        )                             as score,
        |       nvl(re.first_time, log.min_time)   as first_time,
        |       nvl(log.min_time, re.last_time)    as last_time
        |
        |from pre_relation re
        |         full join cur_log_group log
        |                   on re.deviceid = log.deviceid and re.account = log.account
        |""".stripMargin)

    // 从这个结果中，去掉那些已经有了关联账号的空设备
    frame.where("account is null").createTempView("t_null")
    frame.where("account is not null").createTempView("t_not_null")

    val result = session.sql(
      """
        |select
        |       nvl(t_not_null.deviceid, t_null.deviceid)         as deviceid,
        |       t_not_null.account                                as account,
        |       nvl(t_not_null.score, 0)                          as score,
        |    nvl(t_not_null.first_time,cast (concat(`unix_timestamp`(),'000') as bigint))  as first_time,
        |    nvl(t_not_null.last_time,cast (concat(`unix_timestamp`(),'000') as bigint))   as last_time
        |
        |from t_null
        |         full join t_not_null on t_null.deviceid = t_not_null.deviceid
        |
        |""".stripMargin)

    result.createTempView("result")
    session.sql(
      s"""
         |insert into table dwd.device_account_relation partition(dt = '${DT_CUR}')
         |select deviceid,account,score,first_time,last_time
         |from result
         |""".stripMargin)

    session.close()
  }
}
