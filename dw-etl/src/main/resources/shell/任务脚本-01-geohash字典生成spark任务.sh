#!/bin/bash
source /etc/profile

${SPARK_HOME}/bin/spark-submit \
--master yarn          \
--deploy-mode cluster    \
--class bigdata.hermesfuxi.datayi.etl.GeoHashDictGen \
--name "geohash字典生成成功"   \
--driver-memory 1G            \
--executor-memory 1G             \
--executor-cores 1    \
--queue default \
--num-executors 2   \
--conf spark.sql.shuffle.partitions=5  \
/root/datayi.jar 

if [ $? -eq 0 ]
then
 echo "geohash字典生成成功"
else
 echo "geohash字典生成失败"
 echo "geohash字典生成失败" | mail -s 'spark任务失败' 2472854207@qq.com
fi