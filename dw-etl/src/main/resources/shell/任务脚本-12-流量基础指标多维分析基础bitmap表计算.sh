#!/bin/bash
source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${SPARK_HOME}/bin/spark-submit \
--master yarn          \
--deploy-mode cluster    \
--class cn.doitedu.dwetl.TrafficBasicMetricBitmapCalc \
--name 流量多维分析基础bitmap表计算   \
--driver-memory 2G            \
--executor-memory 2G             \
--executor-cores 1    \
--queue default \
--num-executors 2   \
--conf spark.sql.shuffle.partitions=5  \
/root/dw_etl.jar  ${DT_CUR}

if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：流量基础指标多维分析bitmap基础表计算, T日： ${DT_CUR} " | mail -s "spark任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：流量基础指标多维分析bitmap基础表计算, T日： ${DT_CUR} " | mail -s "spark任务失败" 83544844@qq.com
fi

