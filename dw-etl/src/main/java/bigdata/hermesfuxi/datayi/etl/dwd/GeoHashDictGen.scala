package bigdata.hermesfuxi.datayi.etl.dwd

import java.util.Properties

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.SparkSession

/**
 * GEOHASH地理位置字典生成器
 */
object GeoHashDictGen {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val gps2GeoHash = (latitude: Double, longitude: Double) => GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 5);
    session.udf.register("gps2GeoHash", gps2GeoHash)

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    val df = session.read.jdbc("jdbc:mysql://192.168.78.1:3306/realtime", "area_dict", properties)
    df.createTempView("df")
    val result = session.sql(
      """
        |select
        |  gps2GeoHash(BD09_LAT,BD09_LNG) as geohash,
        |  province,
        |  city,
        |  region
        |from df
        |""".stripMargin)
    //      .show()
    result.write.parquet("hdfs://hadoop-master:9000/datayi/dicts/geodict/")
    session.close()
  }

}
