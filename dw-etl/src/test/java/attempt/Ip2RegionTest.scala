package attempt

import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
 * 测试 ip 地域映射
 */
object Ip2RegionTest {
  def main(args: Array[String]): Unit = {

    val searcher = new DbSearcher(new DbConfig(), "dw-etl/src/main/resources/db/ip2region.db")
    val block = searcher.memorySearch("222.64.158.53")
    // 中国|0|上海|上海市|电信
    println(block.getRegion)
  }
}
