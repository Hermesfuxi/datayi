package bigdata.hermesfuxi.datayi.beans

import org.apache.spark.sql.Row

case class AppLogBean(
                       var account: String,
                       var appid: String,
                       var appversion: String,
                       var carrier: String,
                       var deviceid: String,
                       var devicetype: String,
                       var eventid: String,
                       var ip: String,
                       var latitude: Double,
                       var longitude: Double,
                       var nettype: String,
                       var osname: String,
                       var osversion: String,
                       var properties: Map[String, String],
                       var releasechannel: String,
                       var resolution: String,
                       var sessionid: String,
                       var timestamp: Long,
                       var newsessionid: String = "",
                       var location: String = "",
                       var guid: String = "",
                       var newflag: Boolean = false,
                     )

object AppLogBean {
  def row2AppLogBean(row: Row): AppLogBean ={
    try {
      AppLogBean(
        row.getAs[String]("account"),
        row.getAs[String]("appid"),
        row.getAs[String]("appversion"),
        row.getAs[String]("carrier"),
        row.getAs[String]("deviceid"),
        row.getAs[String]("devicetype"),
        row.getAs[String]("eventid"),
        row.getAs[String]("ip"),
        row.getAs[Double]("latitude"),
        row.getAs[Double]("longitude"),
        row.getAs[String]("nettype"),
        row.getAs[String]("osname"),
        row.getAs[String]("osversion"),
        row.getAs[Map[String, String]]("properties"),
        row.getAs[String]("releasechannel"),
        row.getAs[String]("resolution"),
        row.getAs[String]("sessionid"),
        row.getAs[Long]("timestamp"),
        row.getAs[String]("newsessionid"),
      )
    } catch {
      case e: Exception => {
        e.printStackTrace()
        null
      }
    }
  }
}
