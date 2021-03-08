package bigdata.hermesfuxi.datayi.functions

import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.roaringbitmap.RoaringBitmap

/**
 * bitmap分组聚合自定义函数（or 操作）
 */
object BitmapOrAggregation extends Aggregator[Array[Byte], Array[Byte], Array[Byte]] {
  override def zero: Array[Byte] = RoaringBitmapUtil.serialize(new RoaringBitmap())

  override def reduce(b: Array[Byte], a: Array[Byte]): Array[Byte] = {
    val bMap = RoaringBitmapUtil.deserialize(b)
    val aMap = RoaringBitmapUtil.deserialize(a)
    bMap.or(aMap)
    RoaringBitmapUtil.serialize(bMap)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = reduce(b1, b2)

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  override def outputEncoder: Encoder[Array[Byte]] = bufferEncoder
}
