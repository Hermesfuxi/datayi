#!/bin/bash

source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)


if [ $1 ]
then
DT_CUR=$1
fi


${HIVE_HOME}/bin/hive -e "
with tmp as (
select
guid,
datediff(lead(range_start,1,'${DT_CUR}') over(partition by guid order by range_start), if(range_end='9999-12-31','${DT_CUR}',range_end)) as silent_days
from dws.user_active_time_range
where dt='${DT_CUR}' and datediff('${DT_CUR}',range_end)<=30
)

insert into table ads.user_active_silent_days 
select
'${DT_CUR}' as calc_dt , 
count(distinct  if(silent_days between 3 and 4,guid,null)) as slient_3day,
count(distinct  if(silent_days >=5,guid,null)) as slient_5day
from tmp
"



if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：连续沉默天数分布报表计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：连续沉默天数分布报表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 83544844@qq.com
fi

