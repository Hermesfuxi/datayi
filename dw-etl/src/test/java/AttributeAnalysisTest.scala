import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    val filtered = demo.where('eventid isin("e1", "e3", "e5", "e6"))
      .groupBy("guid")
      .agg(array_sort(collect_list(concat_ws("_", lpad('ts, 2, "0"), 'eventid))) as ("evs"))
      .where(concat_ws("", 'evs).rlike(".*?e6.*?"))

    //    filtered.show()
    // 数组分割
    val splitArrayByElement = (array: Array[String], element: String) => {
      val arrayList = new ListBuffer[ListBuffer[String]]()
      var list = new ListBuffer[String]

      for (elem <- array) {
        // 遇到拦截元素
        if (element.equals(elem)) {
          arrayList += list
          list = new ListBuffer[String]
        } else {
          list += elem
        }
      }
      arrayList
    }
    val find_first_attr = (array: mutable.WrappedArray[String], targetEv: String) => {
      val tempArr = array.toArray.map(_.split("_")(1))
      val arrayList = splitArrayByElement(tempArr, targetEv)
      println(arrayList)
      arrayList.map(list => list.head).toArray
    }
    // 首次触点归因
    val find_first_attr_udf = udf(find_first_attr)
    spark.udf.register("find_first_attr", find_first_attr_udf)

    //    DSL 风格
    //    filtered.select('guid, find_first_attr('evs, lit("e6")) as("first_attr")).show(100, false)

    //    SQL 风格
    filtered.createTempView("filtered")
    //    spark.sql(
    //      """
    //        | select
    //        | '首次触点归因' as attr_model_name,
    //        |'e6' as attr_target,
    //        | guid,
    //        | first_attr,
    //        | 100 as attr_weight
    //        | from filtered
    //        | lateral view explode(find_first_attr(evs, 'e6')) o as first_attr
    //        |
    //        |""".stripMargin).show()

    // 不使用注册函数, 使用正则表达式
    //    spark.sql(
    //      """
    //        |select guid,
    //        |(regexp_extract(attribute_list,'(\\d+?):(e\\d)',2)) as list_str
    //        |from (
    //        |         select guid,
    //        |                listStr
    //        |         from (
    //        |                  select guid,
    //        |                         concat_ws(" ", collect_list(concat_ws(":", ts, eventid))) as listStr
    //        |                  from test.attribute_demo
    //        |                  where eventid in ("e1", "e3", "e5", "e6")
    //        |                  group by guid
    //        |              ) a
    //        |         where a.listStr regexp ('.*?e6.*?')
    //        |     ) b
    //        |         lateral view explode(split(listStr, "e6")) e as attribute_list
    //        |     where trim(attribute_list) regexp (':$')
    //        |
    //        |""".stripMargin).show(100, false)


    spark.close()
  }
}
