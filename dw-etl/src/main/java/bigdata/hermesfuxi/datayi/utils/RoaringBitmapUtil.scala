package bigdata.hermesfuxi.datayi.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable

/**
 * 关于 BitMap聚合的实现
 */
object RoaringBitmapUtil {

  def main(args: Array[String]): Unit = {
    val bitmap = RoaringBitmap.bitmapOf(1, 2, 3, 1000);

    bitmap.addN(Array(7, 6), 0, 2)
    val newBitMap = bitmap.limit(3)
    println(newBitMap.toArray.toBuffer)
  }

  /**
   * 序列化： 将 BitMap对象序列化为 Byte数据流
   * @param bitMap 对象
   * @return
   */
  def serialize(bitMap:RoaringBitmap):Array[Byte]={
    val bout: ByteArrayOutputStream = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(bout)
    bitMap.serialize(dataOut)
    bout.toByteArray
  }

  /**
   * 反序列化：将数据流反序列化为 BitMap
   * @param array 接收的数据Byte流
   * @return bitMap
   */
  def deserialize(array: Array[Byte]): RoaringBitmap = {
    val bitmap = new RoaringBitmap()
    val bitArrInput = new ByteArrayInputStream(array)
    val dataInput = new DataInputStream(bitArrInput)
    bitmap.deserialize(dataInput)
    bitmap
  }

  /**
   * 取bitmap中的1的个数（基数）
   * @param array 数据参数
   * @return
   */
  def getCardNum(array:Array[Byte]): Int = {
    val bitmap = deserialize(array)
    bitmap.getCardinality
  }

  def getCardNumByInt(array: mutable.WrappedArray[Int]): Int = {
    val bitmap = RoaringBitmap.bitmapOf(array: _*)
    bitmap.getCardinality
  }

  /**
   * array 转 bitmap，并实现序列化
   */
  val arrayToBitmap = (array: mutable.WrappedArray[Int]) => {
    val bitmap = RoaringBitmap.bitmapOf(array: _*)
    val bout = new ByteArrayOutputStream()

    // 利用jdk自己的objectoutputstream序列化的效率较低
    /*val objOutput = new ObjectOutputStream(bout)
    objOutput.writeObject(bitmap)*/

    // 利用roaringbitmap自带的序列化方法，效率较高
    val dataOutput = new DataOutputStream(bout)
    bitmap.serialize(dataOutput)
    bout.toByteArray
  }
}
