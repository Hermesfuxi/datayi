package datatransfer

import com.google.gson.Gson
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-01-30
 * @desc dws层会话聚合表导入hbase
 *       hbase 建表：
 *       hbase> create 't_session_aggr','f'
 *       -- 表结构设计：
 *       rowkey： 根据分析平台前端设计的查询功能，发现，最频繁的查询条件为 省市区，第二频繁的为：入口页
 *       所以，设计行键结构为：   province:city:region:start_page
 *       family： f
 *       kv结构：整个会话的信息，组装成一个json字符串，作为一个kv即可
 *
 */
object SessionAggrLoader {
  def main(args: Array[String]): Unit = {
    /**
     * 将rdd保存为hfile文件的模板
     */
    val conf = HBaseConfiguration.create()
    conf.set("fs.defaultFS","hdfs://hdp01:8020")
    conf.set("hbase.zookeeper.quorum","hdp01:2181,hdp02:2181,hdp03:2181")
    val job = Job.getInstance(conf)

    val connection = ConnectionFactory.createConnection(conf)
    val hbaseTableName = TableName.valueOf("t_session_aggr")
    val hbase_table = connection.getTable(hbaseTableName)
    val locator: RegionLocator = connection.getRegionLocator(hbaseTableName)
    val admin = connection.getAdmin

    HFileOutputFormat2.configureIncrementalLoad(job, hbase_table, locator)

    // 调用生成HFILE的方法
    convertHfile(job)

    // 调用导入方法，将生成好的HFile导入Hbase表
    // 或者使用命令脚本来导入：
    // bin/hbase org.apache.hadoop.hbase.tool.LoadIncrementalHFiles  /hfile_tmp/dws_sesssion_aggr/  t_session_aggr
    bulkLoad(job,admin,hbase_table,locator)

    admin.close()
    connection.close()

  }

  def bulkLoad(job:Job,admin:Admin,hbaseTable:Table,locator:RegionLocator): Unit ={

    // 将生成好的HFILE导入hbase的目标表
    new LoadIncrementalHFiles(job.getConfiguration).doBulkLoad(new Path("/hfile_tmp/dws_sesssion_aggr/"),admin,hbaseTable,locator)

  }




  def convertHfile(job:Job): Unit ={

    val spark = SparkSession.builder()
      .appName("会话聚合表导入hbase")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    // 读hive中源表
    val table = spark.read.table("dws.tfc_app_agr_session").where("dt='2021-01-10'")

    implicit class MyStr(s:Any){
      def myToString() ={
        if(s==null || s.equals("UNKNOWN") || s.equals("内网IP") || s.equals("0")) "\\N" else s.toString
      }
    }

    // 将数据整理成KV结构
    val kvRDD = table.rdd.mapPartitions(iter=>{
      val gson = new Gson()
      iter.map(row => {

        val bean = new SessionBean()
        bean.set(
          row.get(0).myToString,
          row.get(1).myToString,
          row.get(2).myToString,
          row.get(3).myToString,
          row.get(4).myToString,
          row.get(5).myToString,
          row.get(6).myToString,
          row.get(7).myToString,
          row.get(8).myToString,
          row.get(9).myToString,
          row.get(10).myToString,
          row.get(11).myToString,
          row.get(12).myToString,
          row.get(13).myToString,
          row.get(14).myToString,
          row.get(15).myToString
        )

        val K = DigestUtils.md5Hex(bean.getProvince)+DigestUtils.md5Hex(bean.getCity)+DigestUtils.md5Hex(bean.getRegion)+DigestUtils.md5Hex(bean.getStart_page)
        val V = gson.toJson(bean)
        (K,"f","q",V)
      })
    })
      // 对数据按照  行键，列族名，列名 排序
      .sortBy(tp=>(tp._1,tp._2,tp._3))

    // 将KV数据整理成hbase的指定类型
    val keyValueRDD = kvRDD.map(tp=>{
      val rowkey = new ImmutableBytesWritable(tp._1.getBytes())
      val keyValue = new KeyValue(tp._1.getBytes(),tp._2.getBytes(),tp._3.getBytes(),tp._4.getBytes())

      (rowkey,keyValue)
    })


    // 将我们自己的数据保存为HFile
    keyValueRDD.saveAsNewAPIHadoopFile("/hfile_tmp/dws_sesssion_aggr/", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)


    spark.close()
  }

}
