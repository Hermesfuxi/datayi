package bigdata.hermesfuxi.datayi.utils

import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSparkSession(appName: String, isLocal: Boolean): SparkSession = {
    val spark = SparkSession.builder().appName(appName).enableHiveSupport()
    if(isLocal){
      spark.master("local[*]")
    }
    spark.getOrCreate()
  }
}
