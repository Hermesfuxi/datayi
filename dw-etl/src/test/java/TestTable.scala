import org.apache.spark.sql.SparkSession

object TestTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql(
      """
        | show patitions
        |""".stripMargin)

//    spark.sql(
//      """
//        |with a as (
//        |    select member_id,
//        |           member_level_id,
//        |           start_dt,
//        |           end_dt
//        |    from test.member_zip
//        |    where dt >= '2021-01'
//        |      and dt < '2021-02'
//        |),
//        |     b as (
//        |         select order_id,
//        |                member_id,
//        |                order_amt,
//        |                create_time
//        |         from test.oms_order
//        |         where dt >= '2021-01'
//        |           and dt < '2021-02'
//        |     )
//        |select a.member_id,
//        |       a.member_level_id,
//        |       count(b.order_id) as cnt,
//        |       sum(b.order_amt) as amt_sum
//        |from a
//        |         join b on a.member_id = b.member_id
//        |    and b.create_time between a.start_dt and a.end_dt
//        |group by a.member_id, a.member_level_id
//        |order by a.member_id, a.member_level_id
//        |""".stripMargin).show()

//    spark.sql(
//      """
//        |
//        |select
//        |guid,
//        |concat(reverse(collect_list(rn))) as rnlist
//        |from (
//        |         with b as (select dt
//        |                    from `test`.active_user_day
//        |                    where dt between date_sub(`current_date`(), 31) and `current_date`()
//        |                    group by dt
//        |         )
//        |         select
//        |             a.guid,
//        |             b.dt,
//        |             `if`(a.dt = b.dt, 1, 0) as rn
//        |         from (
//        |                  select guid,dt
//        |                  from `test`.active_user_day
//        |                  where dt between date_sub(`current_date`(), 31) and `current_date`()
//        |                  group by guid,dt order by guid,dt
//        |              ) a
//        |                  full join b
//        |         order by a.guid, b.dt
//        |         ) c
//        |group by guid
//        |
//        |""".stripMargin).show()

    //    val result = spark.sql(
    //      s"""
    //         |select
    //         |guid,
    //         |concat(reverse(collect_list(rn))) as rn
    //         |from (
    //         |         with b as (select dt
    //         |                    from `test`.active_user_day
    //         |                    where dt between date_sub(`current_date`(), 31) and `current_date`()
    //         |                    group by dt
    //         |         )
    //         |         select
    //         |             a.guid,
    //         |             a.dt,
    //         |             b.dt,
    //         |             if(a.guid = null, 0, 1) as rn
    //         |         from (
    //         |                  select guid, dt
    //         |                  from `test`.active_user_day
    //         |                  where dt between date_sub(`current_date`(), 31) and `current_date`()
    //         |              ) a
    //         |                  full join b on a.dt = b.dt
    //         |         )
    //         |
    //         |""".stripMargin)
    //
    //    result.createTempView("result")
    //    spark.sql(
    //      s"""
    //         | insert into table dwd.user_guid_global partition(dt='${args(0)}')
    //         | select * from result
    //         |""".stripMargin)

    spark.sql(
      """
        |
        |""".stripMargin)

    spark.close()
  }
}
