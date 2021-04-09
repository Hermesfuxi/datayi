package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author hermesfuxi
 *         desc: knn算法实战：手写数字图片识别
 */
object KnnDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 加载样本数据集
    val sample = spark.read.textFile("user-profile/data/knn/samples_vec")
    // 向量化
    val sample_vec = sample.map(s => {
      val arr = s.split(",")
      (arr(arr.length - 1), arr.slice(0, arr.length - 1).map(_.toDouble))
    }).toDF("label", "vec")

    // 加载测试数据集
    val test = spark.read.textFile("user-profile/data/knn/test_vec")
    val test_vec = test.map(s => {
      val arr = s.split(",")
      (arr(arr.length - 1), arr.slice(0, arr.length - 1).map(_.toDouble))
    }).toDF("image_name", "vec")

    // 对每个测试数据，求与每个样本向量之间的距离

    // sample_vec.show(100,false)
    // test_vec.show(100,false)

    sample_vec.createTempView("sample")
    test_vec.createTempView("test")

    // 找每个测试数据距离最近的3个样本

    val eudist = (vec1: mutable.WrappedArray[Double], vec2: mutable.WrappedArray[Double]) => {
      // 1,1,0,1,0,...
      // 0,1,1,0,1,...
      // =zip=> [(1,0),(1,1),(0,1),.....] =>map=>  [1,0,1,...]
      vec1.zip(vec2).map(tp => Math.pow(tp._2 - tp._1, 2.0)).sum
    }
    spark.udf.register("eudist", eudist)

    spark.sql(
      """
        |
        |select
        |sample.label,
        |test.image_name,
        | -- sample.vec as sample_vec,
        | -- test.vec as test_vec
        |eudist(sample.vec,test.vec) as dist
        |
        |from sample cross join test
        |
        |""".stripMargin)
      .createTempView("tmp")


    // 找每个测试数据最近的3个样本
    spark.sql(
      """
        |select
        |label,
        |image_name
        |
        |from
        |(
        |select
        |  label,
        |  image_name,
        |  row_number() over(partition by image_name order by dist) as rn
        |from tmp
        |) o
        |where rn <=3
        |
        |
        |""".stripMargin).createTempView("tmp2")



    // 计算3个最近样本中，哪个类别占比更大
    /**
     * +-----+----------+
     * |label|image_name|
     * +-----+----------+
     * |6    |b         |
     * |6    |b         |
     * |7    |b         |
     * |7    |a         |
     * |7    |a         |
     * |7    |a         |
     * +-----+----------+
     */
    spark.sql(
      """
        |
        |select
        |image_name,
        |label
        |
        |from tmp2
        |group by label,image_name
        |having count(1)>=2
        |
        |
        |
        |""".stripMargin).show(100, false)

    spark.close()
  }

}
