#!/bin/bash
######################################
#  @author :  hermesfuxi
#  @date   :  2021-01-21
#  @desc   :  用户日活记录表计算
######################################

source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
insert into table dws.user_active_day partition(dt='${DT_CUR}')
select
 guid
from dws.tfc_app_agr_session where dt='${DT_CUR}'
group by guid
"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
else
 echo "任务失败"
 echo "任务失败： $(date)：用户日活记录表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 2472854207@qq.com
fi

