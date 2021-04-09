package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
 * @author hermesfuxi
 * desc 相关性计算
 */
object CorrelationCalcDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("逻辑回归")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val sample = spark.read.option("header", value = true).option("inferSchema", value = true).csv("user-profile/data/corr/sample")
    // sample.show(100,false)

    val arr2vec = udf((arr:mutable.WrappedArray[Double])=>{
      Vectors.dense(arr.toArray)
    })

    val vec = sample.select(arr2vec(array('material,'area,'floor,'price)) as "features")

    val cor: DataFrame = Correlation.corr(vec, "features")

    cor.show(100,truncate = false)


    spark.close()


  }


}
