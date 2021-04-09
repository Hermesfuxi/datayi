package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author hermesfuxi
 * desc 线性回归demo案例，房价预测
 */
object LinearRegressionDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("线性回归")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val vec = udf((arr:mutable.WrappedArray[Double])=>{Vectors.dense(arr.toArray)})

    val sample = spark.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("user-profile/data/linear/sample/")
      .select('price,vec(array("area","floor")) as "vec")

    val test = spark.read
      .option("header", "true")
      .option("inferSchema", value = true)
      .csv("user-profile/data/linear/test/")
      .select('price,vec(array("area","floor")) as "vec")



    // 构造线性回归算法对象
    val regression = new LinearRegression()
      .setFeaturesCol("vec")
      .setLabelCol("price")
      .setMaxIter(100)
      .setRegParam(0.1)  // 正则化系数,防止过拟合

    // 训练模型
    val model = regression.fit(sample)

    // 用模型对测试数据进行预测
    val predict = model.transform(test)
    predict.cache()
    predict.show(100,truncate = false)



    //  回归模型评估
    // 工具所支持的评估指标有： "mse"均方误差, "rmse"均方根误差, "r2", "mae"平均绝对误差
    val evaluator1 = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("mse")
    val mse = evaluator1.evaluate(predict)

    val evaluator2 = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator2.evaluate(predict)


    val evaluator3 = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("r2")
    val r2 = evaluator3.evaluate(predict)


    val evaluator4 = new RegressionEvaluator()
      .setLabelCol("price")
      .setPredictionCol("prediction")
      .setMetricName("mae")
    val mae = evaluator4.evaluate(predict)


    println(
      s"""
        |
        |mse: ${mse}
        |rmse: ${rmse}
        |r2: ${r2}
        |mae: ${mae}
        |""".stripMargin)



    predict.show(100,false)

    spark.close()
  }

}
