package attempt

import java.text.DecimalFormat

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * desc 归因分析
 * 首次触点归因：待归因事件中，最早发生的事，被认为是导致业务结果的唯一因素
 * 末次触点归因：待归因事件中，最近发生的事，被认为是导致业务结果的唯一因素
 * 线性归因：待归因事件中，每一个事件都被认为对业务结果产生了影响，影响力平均分摊
 * 位置归因：定义一个规则，比如最早、最晚事件占40%影响力，中间事件平摊影响力
 * 时间衰减归因：越晚发生的待归因事件，对业务结果的影响力越大
 */
object AttributeAnalysisTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val demo = spark.read.table("test.attribute_demo")

    // -- 定义目标事件为 e6   ，待归因事件有： e1  e3  e5
    val filtered = demo.where('eventid.isin("e1", "e3", "e5", "e6"))
      .groupBy("guid")
      .agg(array_sort(collect_list(concat_ws("_", lpad('ts, 2, "0"), 'eventid))) as "evs")
      .where(concat_ws("", 'evs).rlike(".*?e6.*?"))

    filtered.createTempView("demo")

    // 工具方法：
    // 将事件列表，根据目标事件的可能多次发生位置，切割成多个事件列表
    val splitEvList = (evs: Array[String], targetEv: String) => {
      val buffer = new ListBuffer[ListBuffer[String]]
      var listBuffer = new ListBuffer[String]
      for (i <- evs.indices) {
        if (evs(i).equals(targetEv)) {
          buffer += listBuffer
          listBuffer = new ListBuffer[String]
        } else {
          listBuffer += evs(i)
        }
      }
      buffer.toList
    }

    // 首次触点归因自定义函数: 待归因事件中，最早发生的事，被认为是导致业务结果的唯一因素 100%
    val find_first_attr = (evs: mutable.WrappedArray[String], targetEv: String) => {
      val strings = evs.toArray.map(_.split("_")(1))
      val segList: Array[ListBuffer[String]] = splitEvList(strings, targetEv).toArray

      for (elem <- segList) yield elem.head
    }
    spark.udf.register("find_first_attr", find_first_attr)

    val find_first_attr_udf = udf(find_first_attr)
    // sql.functions.lit()函数，直接返回的是字面值: 增加一列 evs，值全为 e6 字符串
    filtered.select('guid, find_first_attr_udf('evs, lit("e6"))).show(100, false)

    // 末次触点归因自定义函数: 待归因事件中，最近发生的事，被认为是导致业务结果的唯一因素 100%
    val find_last_attr = (evs: mutable.WrappedArray[String], targetEv: String) => {
      val list: Array[ListBuffer[String]] = splitEvList(evs.toArray.map(_.split("_")(1)).toArray, targetEv).toArray
      for (elem <- list) yield elem.last
    }
    spark.udf.register("find_last_attr", find_last_attr)


    // 线性归因自定义函数
    val line_attr = (evs: mutable.WrappedArray[String], targetEv: String) => {
      val list: Array[ListBuffer[String]] = splitEvList(evs.toArray.map(_.split("_")(1)), targetEv).toArray
      list.flatMap(seq => {
        val attrEvs: Set[String] = seq.toSet
        val df = new DecimalFormat("0.00")
        attrEvs.map(e => e + "," + df.format(100.0 / attrEvs.size))
      })
    }
    spark.udf.register("line_attr", line_attr)

    // 位置归因自定义函数: 最早、最晚事件占40%影响力，中间事件平摊影响力
    val location_attr = (evs: mutable.WrappedArray[String], targetEv: String) => {
      val list: Array[ListBuffer[String]] = splitEvList(evs.toArray.map(_.split("_")(1)), targetEv).toArray
      list.flatMap(seq => {
        val df = new DecimalFormat("0.00")
        val attrEvs = seq.toSet.toList
        var strList = new ListBuffer[String]
        val size = attrEvs.size
        if (size == 1) {
          strList += (attrEvs.head + "," + df.format(100.0))
        } else if (size == 2) {
          strList += (attrEvs.head + "," + df.format(50.0))
          strList += (attrEvs.last + "," + df.format(50.0))
        } else if (size > 2) {
          strList += (attrEvs.head + "," + df.format(40.0))
          for (i <- 1 to size - 2) {
            strList += (attrEvs(i) + "," + 20.0 / (size - 2))
          }
          strList += (attrEvs.last + "," + df.format(40.0))
        }
        strList
      })
    }
    spark.udf.register("location_attr", location_attr)

    // 时间衰减归因自定义函数
    val time_attr = (evs: mutable.WrappedArray[String], targetEv: String) => {
      val list: Array[ListBuffer[String]] = splitEvList(evs.toArray.map(_.split("_")(1)), targetEv).toArray
      list.flatMap(seq => {
        val df = new DecimalFormat("0.00")
        val attrEvs = seq.toSet.toList
        val size = attrEvs.size
        var strList = new ListBuffer[String]
        // 生成权重数组   1  2  3
        val weightArray = for (i <- 0 until size) yield Math.pow(0.9, i)
        for (i <- 0 until size) yield {
          strList += (attrEvs(i) + "," + df.format((weightArray(i) * 100.0) / weightArray.sum))
        }
        strList
      })
    }
    spark.udf.register("time_attr", time_attr)


    val result = spark.sql(
      """
        |
        |select
        |'首次触点归因' as attr_model_name,
        |'e6' as attr_target,
        |guid,
        |first_attr as eventid,
        |100 as attr_weight
        |from demo lateral view explode(find_first_attr(evs,'e6')) o as first_attr
        |
        |union all
        |
        |select
        |'末次触点归因' as attr_model_name,
        |'e6' as attr_target,
        |guid,
        |last_attr as eventid,
        |100 as attr_weight
        |from demo lateral view explode(find_last_attr(evs,'e6')) o as last_attr
        |
        |union all
        |
        |select
        |'线性归因' as attr_model_name,
        |'e6' as attr_target,
        |guid,
        |split(attr_weight,',')[0] as eventid,
        |split(attr_weight,',')[1] as attr_weight
        |from demo lateral view explode(line_attr(evs,'e6')) o as attr_weight
        |
        |union all
        |
        |select
        |'位置归因' as attr_model_name,
        |'e6' as attr_target,
        |guid,
        |split(attr_weight,',')[0]  as eventid,
        |split(attr_weight,',')[1]  as attr_weight
        |from demo lateral view explode(location_attr(evs,'e6')) o as attr_weight
        |
        |union all
        |
        |select
        |'时间归因' as attr_model_name,
        |'e6' as attr_target,
        |guid,
        |split(attr_weight,',')[0]  as eventid,
        |split(attr_weight,',')[1]  as attr_weight
        |from demo lateral view explode(time_attr(evs,'e6')) o as attr_weight
        |
        |""".stripMargin)

    result.show()

    /**
     * +---------------+-----------+----+-------+-----------+
     * |attr_model_name|attr_target|guid|eventid|attr_weight|
     * +---------------+-----------+----+-------+-----------+
     * |首次触点归因   |e6         |g001|e1     |100        |
     * |首次触点归因   |e6         |g001|e5     |100        |
     * |首次触点归因   |e6         |g002|e3     |100        |
     * |末次触点归因   |e6         |g001|e5     |100        |
     * |末次触点归因   |e6         |g001|e5     |100        |
     * |末次触点归因   |e6         |g002|e5     |100        |
     * |线性归因       |e6         |g001|e1     |33.33      |
     * |线性归因       |e6         |g001|e3     |33.33      |
     * |线性归因       |e6         |g001|e5     |33.33      |
     * |线性归因       |e6         |g001|e5     |100.00     |
     * |线性归因       |e6         |g002|e3     |33.33      |
     * |线性归因       |e6         |g002|e1     |33.33      |
     * |线性归因       |e6         |g002|e5     |33.33      |
     * |位置归因       |e6         |g001|e1     |40.00      |
     * |位置归因       |e6         |g001|e3     |20.0       |
     * |位置归因       |e6         |g001|e5     |40.00      |
     * |位置归因       |e6         |g001|e5     |100.00     |
     * |位置归因       |e6         |g002|e3     |40.00      |
     * |位置归因       |e6         |g002|e1     |20.0       |
     * |位置归因       |e6         |g002|e5     |40.00      |
     * |时间归因       |e6         |g001|e1     |36.90      |
     * |时间归因       |e6         |g001|e3     |33.21      |
     * |时间归因       |e6         |g001|e5     |29.89      |
     * |时间归因       |e6         |g001|e5     |100.00     |
     * |时间归因       |e6         |g002|e3     |36.90      |
     * |时间归因       |e6         |g002|e1     |33.21      |
     * |时间归因       |e6         |g002|e5     |29.89      |
     * +---------------+-----------+----+-------+-----------+
     */

    spark.close()

  }
}
