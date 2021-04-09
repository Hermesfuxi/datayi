package bigdata.hermesfuxi.datayi.profile

import java.io.{BufferedWriter, File, FileWriter}

import javax.imageio.ImageIO

/**
 * @author hermesfuxi
 *         desc: k近邻算法Demo
 */
object Image2VectorUtil {
  def main(args: Array[String]): Unit = {

    val sampleDir = new File("user-profile/data/knn/test/")

    val writer = new BufferedWriter(new FileWriter("user-profile/data/knn/test_vec/test.csv"))

    val files = sampleDir.listFiles()
    val sb = new StringBuilder()

    for (f <- files) {
      val image = ImageIO.read(f)
      val label = f.getName.split("_")(1)

      for (i <- 0 until 32) {
        for (j <- 0 until 32) {
          sb.append((if (image.getRGB(j, i) == -1) "0" else "1") + ",")
        }

      }

      writer.write(sb.append(label).toString())
      writer.newLine()
      sb.clear()

    }

    writer.close()
  }

}
