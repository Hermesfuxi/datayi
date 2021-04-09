#!/bin/bash
source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
INSERT INTO TABLE dws.user_retention
SELECT
'${DT_CUR}' AS calc_dt,
first_dt,
datediff('${DT_CUR}',first_dt) as retention_days,
count(1) as retention_users

FROM dws.user_active_time_range 

WHERE dt='${DT_CUR}' 
  AND datediff('${DT_CUR}',first_dt) <= 30 
  AND range_end='9999-12-31'

GROUP BY datediff('${DT_CUR}',first_dt),first_dt
"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
 echo "任务成功：  $(date) ：新用户留存分析表计算, T日： ${DT_CUR} " | mail -s "hive任务成功" 83544844@qq.com
else
 echo "任务失败"
 echo "任务失败：  $(date) ：新用户留存分析表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 83544844@qq.com
fi

