package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * 用户连续活跃时间区间记录表计算：
 * 设计思路：设计一张拉链表（时间区间），每天都是全量，只保留三天内的数据
 * 今天的用户活跃表 与 昨天的用户连续活跃时间区间记录表 关联 ： 新来的插入、没来的更新 endTime 、老客新来的插入、一直就在的不用管时间、
 */
object UserActiveTimeRange {
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

    /*
        拿着T-1日的“连续活跃区间记录表”  FULL JOIN    日活T日
        用9999-12-31表示持续到今天，可以有效的减少表的日常更新写入
        逻辑：新来的插入、没来的更新 endTime 、一直就在的不用管、老客新来的插入新纪录
        （1）处理现有记录：分情况取值：
            所有非9999-12-31区间，不用做任何改变
            对于9999-12-31区间，则看如下情况：
                如果T-1日有，T日无，则区间结束日9999-12-31改成T-1（没来的更新 endTime）
                如果T-1日有，T日有，则无需更改（一直就在的不用管）
            对于历史上没有的新用户，则直接生成一条新的记录（新来的插入）

        （2）处理T-1日没有9999-12-31这种区间的人，而T日到访，则单独生成一条新纪录（老客新来的插入新纪录：FULL JOIN无法生成的）
        最后将（1） UNION ALL  （2） 即得到最终更新后的T日连续区间记录表全量
     */
    val tableA = spark.sql(s"select first_dt, guid, range_start, range_end, dt from dws.user_active_time_range where dt = '${DT_PRE}'")
//    tableA.show()
    tableA.createTempView("a")

    val tableB = spark.sql(s"select guid, dt from dws.user_active_day where dt = '${DT_CUR}'")
//    tableB.show()
    tableB.createTempView("b")

//    spark.sql("select guid from a where range_end = '9999-12-31'").show()

    val tableC = spark.sql(s"select first_dt, guid from a where guid not in (select guid from a where range_end = '9999-12-31') group by first_dt, guid")
//    tableC.show()
    tableC.createTempView("c")

    val result1 = spark.sql(
      s"""
         |select
         |       nvl(a.first_dt, b.dt)    as first_dt,
         |       nvl(a.guid, b.guid)      as guid,
         |       nvl(a.range_start, b.dt) as range_start,
         |       case
         |           when a.guid is null and b.guid is not null then '9999-12-31'
         |           when a.guid is not null and a.range_end = '9999-12-31' and b.guid is null then a.dt
         |           else a.range_end
         |           end                  as range_end
         |from a
         |         full join b
         |                   on a.guid = b.guid
         |
         |""".stripMargin)

//    result1.show()

    val result2 = spark.sql(
      """
        |select
        |       c.first_dt,
        |       c.guid,
        |       b.dt         as range_start,
        |       '9999-12-31' as range_end
        |from c
        |         inner join b
        |                    on c.guid = b.guid
        |""".stripMargin)

//    result2.show()

    val result: Dataset[Row] = result1.unionAll(result2)

    // TODO numRows = 50 就运行卡死的问题
    result.write.json("table.json")
//    result.show(50)
//    result.createTempView("result")
//    spark.sql(
//      s"""
//         | insert into table dws.user_active_time_range partition(dt='${DT_CUR}')
//         | select * from result
//         |""".stripMargin)

    spark.close()
  }
}
/* 完整的sql
with
a as (
    select first_dt, guid, range_start, range_end, dt from dws.user_active_time_range where dt = '${DT_PRE}'
),
b as (select guid, dt from dws.user_active_day where dt = '${DT_CUR}')

select
       nvl(a.first_dt, b.dt)    as first_dt,
       nvl(a.guid, b.guid)      as guid,
       nvl(a.range_start, b.dt) as range_start,
       case
           when a.guid is null and b.guid is not null then '9999-12-31'
           when a.guid is not null and a.range_end = '9999-12-31' and b.guid is null then a.dt
           else a.range_end
           end                  as range_end
from a
         full join b
                   on a.guid = b.guid

union all

select
       c.first_dt,
       c.guid,
       b.dt         as range_start,
       '9999-12-31' as range_end
from (select
             first_dt,
             guid
      from a
      where guid not in (select guid from a where range_end = '9999-12-31')
      group by first_dt, guid
     ) c
         inner join b
                    on c.guid = b.guid
 */
