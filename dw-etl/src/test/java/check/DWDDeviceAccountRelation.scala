package check

import org.apache.spark.sql.SparkSession

/**
 * 设备关联表数据的质量检查
 */
object DWDDeviceAccountRelation {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    /*  -- 结果数据的质量检查代码
         -- 按条数统计如果大于按账号统计，且条数>1，则说明相同设备上，有带账号，也有不带账号的
         期望为空
     */
    // TODO 有问题，期望为空，现在不为空
    sparkSession.sql(
      """
        |         select
        |         deviceid
        |         from dwd.device_account_relation
        |         group by deviceid
        |         having count(1) > count(account) and count(1) > 1
        |""".stripMargin).show()

    /**
     * -- 数值质量检查代码
     * 验证 ods中源表的 deviceid 基数 与 设备账号关联评分表 中的 deviceid 基数
     */
    sparkSession.sql(
      """
        | select  count(distinct deviceid) from ods.event_app_log where dt='2021-01-18'
        | union all
        | select  count(distinct deviceid) from dwd.device_account_relation where dt='2021-01-18'
        |""".stripMargin).show()

    sparkSession.close()
  }
}
