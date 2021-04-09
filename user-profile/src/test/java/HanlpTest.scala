import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term

object HanlpTest {
  def main(args: Array[String]): Unit = {

    val cmt = "我有一头小毛驴我从来也不骑，有一天我心血来潮骑着去赶集，手里拿着皮鞭心里正得意，不知怎么哗啦啦摔了一身泥，抬头一看到了多易"

    val str = HanLP.convertToPinyinFirstCharString(cmt, ",", false)
    val str1 = HanLP.convertToTraditionalChinese(cmt)

    val terms: util.List[Term] = HanLP.segment(cmt)
    import scala.collection.JavaConversions._
    terms.foreach(t=>print(t.word+","))

    println(str)
    println(str1)

  }

}
