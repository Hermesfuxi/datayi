package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
 * @author hermesfuxi
 * desc 逻辑回归算法demo，手写数字图片识别
 */
object LogisticRegressionDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("逻辑回归")
      .getOrCreate()

    import spark.implicits._

    // 加载样本集
    val sample = spark.read.textFile("user-profile/data/knn/samples_vec/sample.csv")
    val sample_vec = sample.map(s => {
      val arr = s.split(",")
      // 将label=6 变为0.0   label=7变为 1.0
      (arr(0).toDouble-6,Vectors.dense(arr.tail.map(_.toDouble)))
    }).toDF("label","features")

    // 加载测试集
    val test = spark.read.textFile("user-profile/data/knn/test_vec/test.csv")
    val test_vec = test.map(s => {
      val arr = s.split(",")
      (arr(0),Vectors.dense(arr.tail.map(_.toDouble)))
    }).toDF("image_name","features")


    // 构造逻辑回归算法对象
    val regression = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(0.1)
      .setMaxIter(100)

    // 训练模型
    val model = regression.fit(sample_vec)

    // 测试模型
    val predict = model.transform(test_vec)

    predict.show(100,truncate = false)

    spark.close()

  }

}
