#!/bin/bash
######################################
#  @author :  hermesfuxi
#  @date   :  2021-01-21
#  @desc   :  app日志数据的ods到dwd计算
######################################

source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $# -eq 1 ]
then
DT_CUR=$1
fi

${SPARK_HOME}/bin/spark-submit \
--master yarn          \
--deploy-mode cluster    \
--class bigdata.hermesfuxi.datayi.etl.dwd.EventAppLog2DwdTable   \
--name 'eventAppLog2DwdTable'   \
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
else
 echo "任务失败"
 echo "任务失败：  `date` ：app日志数据的ods到dwd计算, T日： ${DT_CUR} " | mail -s "spark任务失败" 2472854207@qq.com
fi
