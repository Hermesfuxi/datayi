#!/bin/bash

source /etc/profile

DT_PRE=$(date -d'-2 day' +%Y-%m-%d)
DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
DT_PRE=`date -d"${DT_CUR} -1day" +%Y-%m-%d`
fi


${HIVE_HOME}/bin/hive -e "
with a as (
select * from dws.user_active_time_range where dt='${DT_PRE}'
)
,b as (
-- 取当日的活跃用户guid
select guid,dt from dws.user_active_day where dt='${DT_CUR}'
)

INSERT INTO TABLE dws.user_active_time_range PARTITION(dt='${DT_CUR}')

select
nvl(a.first_dt,b.dt) as first_dt,
nvl(a.guid,b.guid) as guid,
nvl(a.range_start,b.dt) as range_start,
case 
  when a.range_end = '9999-12-31' and b.guid is null then a.dt
  when a.range_end is null and b.guid is not null then '9999-12-31'
  else a.range_end
end  as range_end

from a full join b
on a.guid=b.guid 


union all

SELECT
t1.first_dt as first_dt,
t1.guid as guid,
b.dt  as range_start,
'9999-12-31' as range_end

FROM 
(
  select
    first_dt,guid
  from a
  group by guid,first_dt
  having max(range_end) != '9999-12-31'
) t1

join b  on  t1.guid=b.guid

"



if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：连续活跃区间记录表计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 2472854207@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：连续活跃区间记录表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 2472854207@qq.com
fi

