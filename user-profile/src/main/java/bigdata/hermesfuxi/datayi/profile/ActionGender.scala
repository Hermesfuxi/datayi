package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author hermesfuxi
 *         desc 行为性别预测
 */
object ActionGender {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("用户流失风险预测")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val vec = udf((arr: mutable.WrappedArray[Double]) => {
      Vectors.dense(arr.toArray)
    })

    val sample = spark.read.option("header", "true").option("inferSchema", value = true).csv("user-profile/data/gender/sample")

    // 离散取值特征向量化
    val sample_vec1 = sample.select('label, vec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3)) as "vec")

    // 连续取值特征向量化
    val sample_vec2 = sample.select('label, vec(array('day30_buy_cnts, 'day30_buy_amt)) as "vec")

    // 构建朴素贝叶斯算法
    val bayes = new NaiveBayes()
      .setFeaturesCol("vec")
      .setLabelCol("label")
      .setSmoothing(1.0)
    // 针对离散特征进行模型训练
    val bayesModel = bayes.fit(sample_vec1)


    // 由于购买次数和购买金额的特征值，值域范围相差很大，需要对这两个特征分别规范化，缩放到0-1之间
    val scalar = new MinMaxScaler()
      .setInputCol("vec")
      .setOutputCol("vec_norm")
    // 针对连续特征进行模型训练
    val scalarModel = scalar.fit(sample_vec2)
    val sample_vec2_norm = scalarModel.transform(sample_vec2).drop("vec")

    // 训练逻辑回归模型
    val logisticRegression = new LogisticRegression()
      .setFeaturesCol("vec_norm")
      .setLabelCol("label")
      .setRegParam(0.1)
      .setMaxIter(100)
    val logisticRegressionModel = logisticRegression.fit(sample_vec2_norm)


    /**
     * 对测试集进行测试
     */
    // 加载测试集
    val test = spark.read.option("header", "true").option("inferSchema", value = true).csv("user-profile/data/gender/test")

    // 测试集：离散取值特征
    val test_vec1 = test.select('label, vec(array('category1, 'category2, 'category3, 'brand1, 'brand2, 'brand3)) as "vec")
    // 用贝叶斯模型预测
    val bayesPredict = bayesModel.transform(test_vec1)
    bayesPredict.show(100, truncate = false)

    // 测试集：连续取值特征
    val test_vec2 = test.select('label, vec(array('day30_buy_cnts, 'day30_buy_amt)) as "vec")
    val scalarModel2 = scalar.fit(test_vec2)
    val test_vec2_norm = scalarModel2.transform(test_vec2)
    // 用逻辑回归模型预测
    val logisticPredict = logisticRegressionModel.transform(test_vec2_norm)
    logisticPredict.show(100, truncate = false)

    spark.close()
  }
}
