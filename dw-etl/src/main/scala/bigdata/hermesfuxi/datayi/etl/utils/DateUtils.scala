package bigdata.hermesfuxi.datayi.etl.utils

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object DateUtils {
  def getPlusFormatDate(day: Int, dateTimeStr: String, formatStr: String): String = {
    // "yyyy/MM/dd HH:mm:ss"
    val dtf = DateTimeFormatter.ofPattern(formatStr)
    // "2019/11/20 15:23:46"
    val localDate = LocalDate.parse(dateTimeStr, dtf)
    localDate.plusDays(day).format(dtf)
  }

  def getDateMonth(dateTimeStr: String, formatStr: String): Int = {
    // "yyyy/MM/dd HH:mm:ss"
    val dtf = DateTimeFormatter.ofPattern(formatStr)
    // "2019/11/20 15:23:46"
    val localDate = LocalDate.parse(dateTimeStr, dtf)
    localDate.getMonth.getValue
  }

  def getLongTime(dateTimeStr: String, formatStr: String): Long = {
    // "yyyy/MM/dd HH:mm:ss"
    val dtf = DateTimeFormatter.ofPattern(formatStr)
    // "2019/11/20 15:23:46"
    val date: LocalDateTime = LocalDateTime.parse(dateTimeStr, dtf)
    date.toEpochSecond(ZoneOffset.of("+8")) // 秒级
  }
}
