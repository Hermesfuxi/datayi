package attempt

import org.apache.spark.sql.SparkSession

/**
 * 测试拉链表
 */
object TestZipTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        |with a as (
        |    select oid,
        |           amount,
        |           status,
        |           start_dt,
        |           end_dt,
        |           dt
        |    from test.lalian
        |    where dt = '2021-01-22'
        |),
        |     b as (
        |         select oid,
        |                amount,
        |                status,
        |                '2021-01-23' as start_dt,
        |                '2021-01-23' as end_dt
        |         from test.add
        |         where dt = '2021-01-23'
        |     )
        |select a.oid,
        |       a.amount,
        |       a.status,
        |       a.start_dt,
        |       if(b.oid is not null and a.end_dt = '9999-12-31', a.dt, a.end_dt) as end_dt
        |from a
        |         left join b
        |                   on a.oid = b.oid
        |union
        |-- 插入
        |select oid,
        |       amount,
        |       status,
        |       '2021-01-23' as start_dt,
        |       '2021-01-23' as end_dt
        |from test.add
        |where dt = '2021-01-23';
        |
        |""".stripMargin).show()

    spark.close()
  }
}
