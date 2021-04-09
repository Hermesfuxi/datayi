#!/bin/bash
######################################
#  @author :  hermesfuxi
#  @date   :  2021-01-21
#  @desc   :  App流量统计-会话聚合表计算
######################################

# -- 建表SQL
# DROP TABLE IF EXISTS dws.app_flow_agg_session;
# CREATE TABLE dws.app_flow_agg_session(
#    guid         string,
#    session_id   string,
#    start_time   bigint,
#    end_time     bigint,
#    start_page   string,
#    end_page     string,
#    pv_cnt       int,
#    isnew        string,
#    hour_range   int,
#    location      string,
#    device_type  string,
#    os_name      string,
#    release_ch   string
# )
# partitioned by (dt string)
# stored as parquet
# ;

source /etc/profile

DT_CUR=$(date -d'-1 day' +%Y-%m-%d)

if [ $1 ]
then
DT_CUR=$1
fi

${HIVE_HOME}/bin/hive -e "
set hive.vectorized.execution.enabled=false;

-- 计算SQL
WITH tmp AS 
(
SELECT
   guid ,
   sessionid as session_id,
   \`timestamp\`  as ts,
   isnew as isnew,
   eventid as eventid,
   first_value(properties['pageId']) over(partition by guid,sessionid order by if(eventid='pageView',\`timestamp\`,2000000000000)) as start_page,
   first_value(properties['pageId']) over(partition by guid,sessionid order by if(eventid='pageView',\`timestamp\`,0) desc) as end_page,
   first_value(country) over(partition by guid,sessionid order by \`timestamp\`) as country,
   first_value(province) over(partition by guid,sessionid order by \`timestamp\`) as province,
   first_value(city) over(partition by guid,sessionid order by \`timestamp\`) as city,
   first_value(region) over(partition by guid,sessionid order by \`timestamp\`) as region,
   first_value(devicetype) over(partition by guid,sessionid order by \`timestamp\`) as device_type,
   first_value(osname) over(partition by guid,sessionid order by \`timestamp\`) as os_name,
   first_value(releasechannel) over(partition by guid,sessionid order by \`timestamp\`) as release_ch

FROM dwd.event_app_detail 
WHERE dt='${DT_CUR}'
)

INSERT INTO TABLE dws.app_flow_agg_session PARTITION(dt='${DT_CUR}')
SELECT
  guid,
  session_id,
  min(ts) as start_time,
  max(ts) as end_time,
  min(start_page) as start_page,
  min(end_page) as end_page,
  count(if(eventid='pageView',1,null)) as pv_cnt,
  min(isnew)  as isnew,
  hour(from_unixtime(min(cast(ts/1000 as bigint))))  as  hour_range,
  min(country) as country,
  min(province) as province,
  min(city) as city,
  min(region) as region,
  min(device_type) as device_type,
  min(os_name) as os_name,
  min(release_ch) as release_ch
FROM tmp
GROUP BY guid,session_id

"

if [ $? -eq 0 ]
then
 echo "任务执行成功"
else
 echo "任务失败"
 echo "任务失败：  $(date) ：App流量统计-会话聚合表计算, T日： ${DT_CUR} " | mail -s "hive任务失败" 2472854207@qq.com
fi

