#!/bin/bash

# 活跃状态bitmap记录表更新任务
source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)


if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
insert into table dws.active_app_actstatus_bitmap partition(dt='2021-01-11')
select
nvl(a.guid,b.guid) as guid,
case 
  when a.guid is not null and b.guid is not null then  (1073741823&bitmap)*2+1
  when a.guid is not null and b.guid is null     then  (1073741823&bitmap)*2
  else 1
end as bitmap

from 

(select guid,bitmap from dws.active_app_actstatus_bitmap where dt='2021-01-10' )a
full join 
(select * from dws.user_active_day where dt='2021-01-11') b
on a.guid = b.guid

"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：活跃状态bitmap记录表更新计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：活跃状态bitmap记录表更新计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 83544844@qq.com
fi

