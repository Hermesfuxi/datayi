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
max(datediff(if(range_end='9999-12-31','${DT_CUR}',range_end),if(datediff('${DT_CUR}',range_start)>30,date_sub('${DT_CUR}',30),range_start))) as max_act_days
from dws.user_active_time_range
where dt='${DT_CUR}' and datediff('${DT_CUR}',range_end)<=30
group by guid
)

insert into table ads.user_active_continuous_distribute
select
${DT_CUR}  as calc_dt,
count(if(max_act_days >=1,1,null)) as days_1_10,
count(if(max_act_days >=10,1,null)) as days_10_20,
count(if(max_act_days >=20,1,null)) as days_20
from tmp

"



if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：连续活跃天数分布表计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：连续活跃天数分布表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 83544844@qq.com
fi

