#!/bin/bash
source /etc/profile

# 每天自动入库昨天的日志信息
DT=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT=$1
fi

${HIVE_HOME}/bin/hive -e "
load data inpath '/datayi/logdata/app/${DT}' into table ods.event_app_log partition(dt='${DT}')
"

if [ $? -eq 0 ]
then
  echo "${DT} app埋点日志，入库成功"
else
  echo "${DT} app埋点日志，入库失败" | mail -s "${DT} app埋点日志，入库失败" 2472854720@qq.com
fi