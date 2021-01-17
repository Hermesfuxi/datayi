package bigdata.hermesfuxi.datayi.etl

import org.apache.spark.sql.SparkSession

object TrafficBasicMetricTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("多维分析计算（利用bitmap做逐层聚合）")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()



    spark.close()
  }

}
