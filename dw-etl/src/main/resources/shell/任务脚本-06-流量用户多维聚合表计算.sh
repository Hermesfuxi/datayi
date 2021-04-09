#!/bin/bash
######################################
#  @author :  深思海的男人
#  @date   :  2021-01-15
#  @desc   :  App流量统计-用户聚合表计算
######################################

source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
INSERT INTO TABLE dws.app_flow_agg_user PARTITION(dt='${DT_CUR}')
SELECT
  guid         ,
  province     ,
  city         ,
  region       ,
  hour_range   ,
  device_type  ,
  os_name      ,
  start_page   ,
  end_page     ,
  isnew        ,
  count(1)                    as  session_cnt  ,  -- 会话次数
  sum(end_time-start_time)    as  time_long    ,  -- 会话时长
  count(if(pv_cnt<2,1,null))  as  jumpout_cnt  ,  -- 跳出次数
  sum(pv_cnt)                 as  pv_cnt          -- 总访问页数
FROM dws.tfc_app_agr_session
WHERE dt='${DT_CUR}'
GROUP BY 
  guid         ,
  province     ,
  city         ,
  region       ,
  hour_range   ,
  device_type  ,
  os_name       ,
  start_page   ,
  end_page     ,
  isnew        
"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
else
 echo "任务失败"
 echo "任务失败：  $(date) ：流量用户多维聚合表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 2472854207@qq.com
fi
