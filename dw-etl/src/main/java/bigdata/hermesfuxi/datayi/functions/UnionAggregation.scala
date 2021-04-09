package bigdata.hermesfuxi.datayi.functions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

class UnionAggregation(encoder:Encoder[Array[String]]) extends Aggregator[Array[String], Array[String], Array[String]]{
  // 聚合的初始值：比如满足：任何 b + zero = b
  override def zero: Array[String] = Array.empty[String]

  // 分区内聚合:合并两个值。用新值直接更新buffer(初始值为zero)，并返回buffer本身，而不是重新new一个
  override def reduce(b: Array[String], a: Array[String]): Array[String] = {
    b.union(a).distinct
  }

  // 分区间聚合: 合并分区。依旧是用新值直接更新 buffer(初始值为zero)，并返回buffer本身，而不是重新new一个
  override def merge(b1: Array[String], b2: Array[String]): Array[String] = reduce(b1, b2)

  // 最终结果汇总
  override def finish(reduction: Array[String]): Array[String] = reduction

  // 定义内部缓存类型的编码器（前文提到的编码器，用于spark运算中的内部序列化和反序列化）
  override def bufferEncoder: Encoder[Array[String]] = encoder

  // 定义输出结果类型的编码器： 使用和 bufferEncoder一样的编码器
  override def outputEncoder: Encoder[Array[String]] = bufferEncoder
}
