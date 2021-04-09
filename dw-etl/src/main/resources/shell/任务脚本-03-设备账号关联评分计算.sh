#!/bin/bash
source /etc/profile

DT_PRE=$(date -d'-2 day' +%Y-%m-%d)
DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $# -eq 1 ]
then
DT_CUR=$1
DT_PRE=`date -d"${DT_CUR} -1day"  +%Y-%m-%d`  fi

${SPARK_HOME}/bin/spark-submit \
--master yarn          \
--deploy-mode cluster    \
--class bigdata.hermesfuxi.datayi.etl.DeviceAccountRelationScore \
--name device_account_relation   \
--driver-memory 2G            \
--executor-memory 2G             \
--executor-cores 1    \
--queue default \
--num-executors 2   \
--conf spark.sql.shuffle.partitions=5  \
/root/dw_etl.jar ${DT_CUR} ${DT_PRE} 

if [ $? -eq 0 ]
then echo "任务执行成功"
else
 echo "任务失败"
 echo "任务失败：  `date` ：设备账号关联评分计算, T-1日： ${DT_PRE} ，T日： ${DT_CUR} " | mail -s "spark任务失败" 2472854207@qq.com
fi