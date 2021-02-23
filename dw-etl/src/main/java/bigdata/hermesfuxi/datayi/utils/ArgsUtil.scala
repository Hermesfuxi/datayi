package bigdata.hermesfuxi.datayi.utils

import java.time.LocalDate

object ArgsUtil {
  def initArgs(args: Array[String]): (String, String) = {
    var DT_CUR = ""
    var DT_PRE = ""
    if(args.length <= 0){
      // 默认是 T 为 昨天, T-1 为 前天
      val today = LocalDate.now
      DT_CUR = today.plusDays(-1).toString
      DT_PRE = today.plusDays(-2).toString
    }else if(args.length == 1){
      DT_CUR = args(0)
      DT_PRE = DateUtil.getPlusFormatDate(-1, DT_CUR, "yyyy-MM-dd")
    }else if(args.length > 1){
      DT_CUR = args(0)
      DT_PRE = args(1)
    }
    (DT_CUR, DT_PRE)
  }
}
