#!/bin/bash

######################################
#
#  @author :  深思海的男人
#  @date   :  2021-01-15
#  @desc   :  流量统计，会话聚合表计算
#  @other  
######################################


export JAVA_HOME=/opt/apps/jdk1.8.0_191/
export HIVE_HOME=/opt/apps/hive-3.1.2/
export HADOOP_HOME=/opt/apps/hadoop-3.1.1/
export SPARK_HOME=/opt/apps/spark-2.4.4/


DT_CUR=$(date -d'-1 day' +%Y-%m-%d)


if [ $1 ]
then
DT_CUR=$1
fi

${SPARK_HOME}/bin/spark-submit \
--master yarn          \
--deploy-mode cluster    \
--class cn.doitedu.dwetl.ShareEventBitmapCalc \
--name 流量多维分析基础bitmap表计算   \
--driver-memory 2G            \
--executor-memory 2G             \
--executor-cores 1    \
--queue default \
--num-executors 2   \
--jars /root/mysql-connector-java-5.1.47-bin.jar  \
--driver-library-path /root/mysql-connector-java-5.1.47-bin.jar \ 
--conf spark.sql.shuffle.partitions=5  \
/root/dw_etl.jar  ${DT_CUR}






if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：分享事件多维分析bitmap基础表计算, T日： ${DT_CUR} " | mail -s "spark任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：分享事件多维分析bitmap基础表计算, T日： ${DT_CUR} " | mail -s "spark任务失败" 83544844@qq.com
fi

