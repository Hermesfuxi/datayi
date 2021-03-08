package bigdata.hermesfuxi.datayi.etl.dwd

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * 基于T-1日的"关联得分表" 和 T日的行为日志，得出T日的"关联得分表" dwd.device_account_relation
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

    /**
     * —— 将上表与T-1日的关联评分表full join，分情况取值：
     * 如果 “设备-账号”组合T-1有，T日有，则评分累加
     * 如果 “设备-账号”组合T-1有，T日无，则评分衰减（衰减因子为 0.7 ）
     * 如果 “设备-账号”组合T-1无，T日有，则新增记录
     */
    val frame = session.sql(
      """
        |select
        |       nvl(pre.deviceid, cur.deviceid)     as deviceid,
        |       nvl(pre.account, cur.account)       as account,
        |       if(pre.deviceid is not null and cur.deviceid is null,
        |           pre.score * 0.7,
        |           nvl(pre.score, 0) + cur.score
        |        )                                  as score,
        |       nvl(pre.first_time, cur.min_time)   as first_time,
        |       nvl(cur.min_time, pre.last_time)    as last_time
        |
        |from pre_relation pre
        |         full join cur_log_group cur
        |                   on pre.deviceid = cur.deviceid and pre.account = cur.account
        |""".stripMargin)

    // 从这个结果中，去掉那些已经有了关联账号的空设备 （即 对一个设备而言，要么有多个账号，要么没账号 ）
    frame.where("account is null").createTempView("t_null")
    frame.where("account is not null").createTempView("t_not_null")

    val result = session.sql(
      """
        |select
        |       nvl(t_not_null.deviceid, t_null.deviceid)         as deviceid,
        |       t_not_null.account                                as account,
        |       nvl(t_not_null.score, 0)                          as score,
        |       nvl(t_not_null.first_time,cast (concat(`unix_timestamp`(),'000') as bigint))  as first_time,
        |       nvl(t_not_null.last_time,cast (concat(`unix_timestamp`(),'000') as bigint))   as last_time
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
