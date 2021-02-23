package bigdata.hermesfuxi.datayi.utils

import java.io.{ByteArrayInputStream, DataInputStream}

import org.roaringbitmap.RoaringBitmap

object RBMUtil {
  def de(arr:Array[Byte]):RoaringBitmap={
    val rbm = new RoaringBitmap()

    val bainput = new ByteArrayInputStream(arr)
    val dataInput = new DataInputStream(bainput)
    rbm.deserialize(dataInput)

    rbm
  }

}
