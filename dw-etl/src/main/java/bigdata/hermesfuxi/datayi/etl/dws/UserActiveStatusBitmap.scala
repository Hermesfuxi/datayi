package bigdata.hermesfuxi.datayi.etl.dws

import bigdata.hermesfuxi.datayi.utils.ArgsUtil
import org.apache.spark.sql.SparkSession

/**
 * 用户连续活跃时间区间记录表计算
 *  借用 bitMap 的思想：用二进制来表示用户活跃状态（1为活跃，0为不活跃）
 *  记录30天，可用位运算来统计活跃相关信息（若记录60天，可使用Long类型，再多就需要使用 bitmap，其中String可用于顶替操作）
 */
object UserActiveStatusBitmap {
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

    val recordDays = 30

    // ---------------- 第一步 初始化（只执行一次）-------------------
    //初始化活跃状态bitmap记录表（只执行一次）,30天内用二进制转换
    // POW(X,Y) 这个函数返回底数为X，指数为Y的值
    // 2的指数（天数为指数）从 0~30 相加（2^0 + 2^1 + 2^2 + ... 2^29） = 活跃状态记录值
//    val initTable = spark.sql(
//      s"""
//         |select guid,
//         |       '${DT_CUR}' as first_dt,
//         |       cast(sum(pow(2, datediff('${DT_CUR}', dt))) as int) as bitmap
//         |from dws.user_active_day
//         |where dt between date_sub('${DT_CUR}', '${recordDays}') and '${DT_CUR}'
//         |group by guid
//         |""".stripMargin)
//
//    initTable.show()
//    initTable.createTempView("result")
//    spark.sql(
//      s"""
//         | insert into table dws.user_active_status_bitmap partition(dt='${DT_CUR}')
//         | select * from result
//         |""".stripMargin)

    // ---------------- 第二步 滚动更新（从第二天开始，每天执行一次）-------------------
    // 更新算法，先join，判断用户是否活跃，如果活跃，则将原来的bitmap*2+1，如果没活跃，则*2
    // *2操作可能会让整数溢出为负数，需要做一个处理，可以将最高两位变成0（int型数据取值范围是[-2^31,2^31-1]）后再乘，不会溢出，且不会影响结果
    // 最高位31位始终是0（我们只取30天）,第30天需要变成0，故先做与运算（2^30-1=1073741823）
    val statusParam = (Math.pow(2, recordDays) -1).toLong

    val result = spark.sql(
      s"""
         |with a as (select guid, first_dt, active_status from dws.user_active_status_bitmap where dt = '${DT_PRE}'),
         |     b as (select guid, dt from dws.user_active_day where dt = '${DT_CUR}')
         |
         |select nvl(a.guid, b.guid)                                                   as guid,
         |       if(a.guid is null and b.guid is not null, b.dt, a.first_dt)           as first_dt,
         |       (${statusParam} & nvl(a.active_status, 0)) * 2 + if(b.guid is null, 0, 1) as active_status
         |from a
         |         full join b on a.guid = b.guid
         |
         |""".stripMargin)

//    result.show()
    result.createTempView("result")
    spark.sql(
      s"""
         | insert into table dws.user_active_status_bitmap partition(dt='${DT_CUR}')
         | select * from result
         |""".stripMargin)

    spark.close()
  }
}
