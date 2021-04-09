package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author hermesfuxi
 *         desc 流式风险标签预测
 */
object LossProb {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("用户流失风险预测")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val vec = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    val sample = spark.read.option("header", "true").option("inferSchema", value = true).csv("user-profile/data/loss/sample")
    val sample_vec = sample.select('label, 'guid, vec(array('cs_3, 'cs_15, 'xf_3, 'xf_15, 'th_3, 'th_15, 'hp_3, 'hp_15, 'cp_3, 'cp_15, 'last_dl, 'last_xf)) as "vec")


    val test = spark.read.option("header", "true").option("inferSchema", value = true).csv("user-profile/data/loss/test")
    val test_vec = test.select('guid, vec(array('cs_3, 'cs_15, 'xf_3, 'xf_15, 'th_3, 'th_15, 'hp_3, 'hp_15, 'cp_3, 'cp_15, 'last_dl, 'last_xf)) as "vec")


    // 构造算法对象
    val regression = new LogisticRegression()
      .setFeaturesCol("vec")
      .setLabelCol("label")
      .setRegParam(0.1)
      .setMaxIter(100)

    val model = regression.fit(sample_vec)

    val predict = model.transform(test_vec).drop("vec")


    predict.show(100, truncate = false)


    spark.close()

  }


}
