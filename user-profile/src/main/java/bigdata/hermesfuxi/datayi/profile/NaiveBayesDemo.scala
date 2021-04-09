package bigdata.hermesfuxi.datayi.profile

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * @author hermesfuxi
 * desc
 *  调用sparkmllib的算法api，需要把数据中的特征，封装成 “特征向量”  --> Vector类型
 *  而Vector类型有两个实现类：SparseVector 和 DenseVector
 *  这两个实现类，只是在底层的数据存储形式上有区别，一个用于存储稀疏性向量，一个用于存储密集型向量
 *  [0,0,0,0,17,0,0,0,23,0,0,0,0,0,0,.....] 这种稀疏型的向量，如果底层用一个数组来存，浪费空间
 *  可以用如下形式存储，更节省空间：(1000,[4,8,20],[17,23,50])   SparseVector
 *  如果向量是这样：[12,23,6,0,19,32,384,600,...] 这种属于密集型向量，直接用一个数组存储即可，DenseVector
 *
 */
object NaiveBayesDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("朴素贝叶斯案例")
      .master("local[*]")
      .getOrCreate()

    /**
     * 模型训练
     */
    val sample = spark.read.option("header", "true").csv("user-profile/data/trait/sample")

    // 将样本数据向量化
    sample.createTempView("sample")
    // name,job,income,age,sex,label
    val sampleFeatures = spark.sql(
      """
        |
        |select
        |name,
        |case
        | when job='老师' then 1
        | when job='程序员' then 2
        | when job='公务员' then 3
        |end as job,
        |case
        | when income between 0 and 10000 then 1
        | when income between 10001 and 20000 then 2
        | when income between 20001 and 30000 then 3
        | when income between 30001 and 40000 then 4
        | else 5
        |end as income,
        |case
        | when age='青年' then 1
        | when age='中年' then 2
        | when age='老年' then 3
        |end as age,
        |if(sex='男',1,2) as sex,
        |cast(if(label='出轨',1,0)  as double) as label
        |
        |from sample
        |
        |""".stripMargin)

    sampleFeatures.createTempView("sample_features")

    val vec = (arr:mutable.WrappedArray[Int])=>{
      Vectors.dense(arr.map(_.toDouble).toArray)
    }
    spark.udf.register("vec",vec)

    val sample_vec = spark.sql(
      """
        |
        |select
        |
        |name,
        |vec(array(job,income,age,sex)) as vec,
        |label
        |
        |from sample_features
        |
        |""".stripMargin)


    // 构造朴素贝叶斯算法对象
    val naiveBayes = new NaiveBayes()
        .setLabelCol("label")
        .setFeaturesCol("vec")
        .setSmoothing(1.0) // 拉普拉斯平滑系数

    // 训练模型
    val model = naiveBayes.fit(sample_vec)
    model.save("user-profile/data/trait/model")


    /**
     * 预测
     */

    val test = spark.read.option("header", "true").csv("user-profile/data/trait/test")
    test.createTempView("test")

    val testVec = spark.sql(
      """
        |
        |select
        |name,
        |vec(array(
        |case
        | when job='老师' then 1
        | when job='程序员' then 2
        | when job='公务员' then 3
        |end,
        |case
        | when income between 0 and 10000 then 1
        | when income between 10001 and 20000 then 2
        | when income between 20001 and 30000 then 3
        | when income between 30001 and 40000 then 4
        | else 5
        |end,
        |case
        | when age='青年' then 1
        | when age='中年' then 2
        | when age='老年' then 3
        |end,
        |if(sex='男',1,2)
        |)) as vec
        |
        |from test
        |
        |""".stripMargin)


    // 加载之前训练好的模型
    val model1 = NaiveBayesModel.load("user-profile/data/trait/model")
    val predict = model1.transform(testVec)

    predict.show(100,truncate = false)

    spark.close()
  }
}
