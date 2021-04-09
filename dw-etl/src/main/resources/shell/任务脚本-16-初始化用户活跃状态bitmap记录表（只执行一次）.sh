#!/bin/bash
# 初始化活跃状态bitmap记录表（只执行一次）
source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
insert into table dws.user_active_status_bitmap partition(dt='${DT_CUR}')
select
guid,
cast(sum(pow(2,datediff('${DT_CUR}',dt))) as int) as bitmap
from dws.user_active_day 
where dt between   date_sub('${DT_CUR}',30) and '${DT_CUR}'
group by guid

"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：活跃状态bitmap记录表计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：活跃状态bitmap记录表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 83544844@qq.com
fi

