package bigdata.hermesfuxi.datayi.functions

import bigdata.hermesfuxi.datayi.utils.RoaringBitmapUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.roaringbitmap.RoaringBitmap

object BitmapOrAggregationFunction extends UserDefinedAggregateFunction {
  // 函数的输入参数的：个数和类型（结构）
  override def inputSchema: StructType = new StructType(Array(new StructField("bitmap", DataTypes.BinaryType)))

  // 中间状态数据缓存的：个数和类型（结构）
  override def bufferSchema: StructType = new StructType(Array(new StructField("buff", DataTypes.BinaryType)))

  // 返回结果的数据类型
  override def dataType: DataType = DataTypes.BinaryType

  // 计算逻辑是否“确定”
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 构造一个空的bitmap作为缓存的初始值
    val bitmap = new RoaringBitmap()
    val bytes = RoaringBitmapUtil.serialize(bitmap)
    buffer.update(0, bytes)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val bufferBytes = buffer.getAs[Array[Byte]](0)
    val inputBytes = input.getAs[Array[Byte]](0)

    // 反序列化
    val bufferBitMap = RoaringBitmapUtil.deserialize(bufferBytes)
    val inputBitMap = RoaringBitmapUtil.deserialize(inputBytes)

    // 或操作
    bufferBitMap.or(inputBitMap)

    // 更新缓存
    buffer.update(0, RoaringBitmapUtil.serialize(bufferBitMap))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Any = buffer.getAs[Array[Byte]](0)
}
