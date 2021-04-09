package bigdata.hermesfuxi.datayi.profile

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author hermesfuxi
 *         desc 商品评论语义分类器
 */
object CommenClassify {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("朴素贝叶斯案例")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**
     * 特征工程
     */
    // 加载样本数据
    val good = spark.read.textFile("D:\\comment_sample\\good").select(lit(0.0) as "label", 'value as "cmt")
    val general = spark.read.textFile("D:\\comment_sample\\general").select(lit(1.0) as "label", 'value as "cmt")
    val poor = spark.read.textFile("D:\\comment_sample\\poor").select(lit(2.0) as "label", 'value as "cmt")

    val sample = good.union(general).union(poor)

    // 分词
    val segment = udf((cmt: String) => {
      val terms: util.List[Term] = HanLP.segment(cmt)
      import scala.collection.JavaConversions._
      terms.map(term => term.word).toArray
    })

    // 加载停止词典
    val stopwords = spark.read.textFile("d:/comment_sample/stopwords").collect().toSet
    val bc = spark.sparkContext.broadcast(stopwords)


    // 过滤停止词
    val sample_words = sample
      .select('label, segment('cmt) as "words")
      .map(row => {

        val stw = bc.value
        val label = row.getAs[Double]("label")
        val words = row.getAs[Seq[String]]("words").filter(w => !stw.contains(w))
        (label, words)
      }).toDF("label", "words")

    // 将词数组，转换成特征向量
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("tf_vec")
      .setNumFeatures(1000000)
    val tfVec = hashingTF.transform(sample_words)

    // 将TF特征值转成TF-IDF特征值
    val idf = new IDF()
      .setInputCol("tf_vec")
      .setOutputCol("tfidf_vec")
    val iDFModel = idf.fit(tfVec)
    val tfidfVec = iDFModel.transform(tfVec).drop("words", "tf_vec")


    // 拆分样本为 “训练集” 80% 和 "测试集“ 20%
    val array: Array[DataFrame] = tfidfVec.randomSplit(Array(0.8, 0.2))
    val trainSample = array(0) // 用于训练
    val testSample = array(1) // 用于测试

    // 构造朴素贝叶斯算法对象
    val bayes = new NaiveBayes()
      .setFeaturesCol("tfidf_vec")
      .setSmoothing(1.0)
      .setLabelCol("label")
    // 训练模型
    val model = bayes.fit(trainSample)

    // 测试
    val predict = model.transform(testSample).drop("tfidf_vec")
    predict.cache()


    // predict.show(100,false)

    // 模型评估：统计准确率
    predict.createTempView("pre")
    spark.sql(
      """
        |
        |select
        |  count(if(label=prediction,1,null)) as correct,
        |  count(1) as total,
        |  count(if(label=prediction,1,null))/count(1) as correct_ratio
        |from pre
        |
        |""".stripMargin).show(100, false)

    /**
     * +-------+------+------------------+
     * |correct|total |correct_ratio     |
     * +-------+------+------------------+
     * |408550 |491499|0.8312326169534424|
     * +-------+------+------------------+
     */

    // 构造二分类评估器，评估AUC指标
    /*val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predict)
    println(auc)*/


    // 详细评估需要自己统计
    // 真好评准确率   真中评准确率   真差评的准确率     预测成了好评中的准确率 ……

    spark.close()
  }
}
