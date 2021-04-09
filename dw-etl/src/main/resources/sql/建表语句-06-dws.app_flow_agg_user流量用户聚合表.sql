-- 流量用户聚合表（日活表）建表
drop table if exists dws.app_flow_agg_user;
create table dws.app_flow_agg_user(
guid          string,
location      string,
hour_range    string   ,
device_type   string,
os_name       string,
start_page    string,
end_page      string,
isnew         string,
session_cnt   int   ,
time_long     bigint,
jumpout_cnt   bigint,
pv_cnt        bigint
)
partitioned by (dt string)
stored as parquet;
