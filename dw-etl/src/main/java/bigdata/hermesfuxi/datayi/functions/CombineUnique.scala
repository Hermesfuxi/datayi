package bigdata.hermesfuxi.datayi.functions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

class CombineUnique(encoder:Encoder[Array[String]]) extends Aggregator[Array[String], Array[String], Array[String]]{

  override def zero: Array[String] = Array.empty[String]

  override def reduce(b: Array[String], a: Array[String]): Array[String] = {
    b.union(a).distinct
  }

  override def merge(b1: Array[String], b2: Array[String]): Array[String] = reduce(b1, b2)

  override def finish(reduction: Array[String]): Array[String] = reduction

  override def bufferEncoder: Encoder[Array[String]] = encoder

  override def outputEncoder: Encoder[Array[String]] = bufferEncoder
}
