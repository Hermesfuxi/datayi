-- 流量基础指标，多维分析报表
drop table if exists dws.app_flow_overview_bitmap_cube;
create table dws.app_flow_overview_bitmap_cube(
  location           string,
  hour_range         string   ,
  device_type        string,
  os_name            string,
  start_page         string,
  end_page           string,
  isnew              string,
  pv_cnt             bigint,
  uv_cnt             bigint,
  session_cnt        bigint,
  time_long          bigint,
  return_user_cnt    bigint,
  jumpout_cnt        bigint,
  jumpout_user_cnt   bigint
)
PARTITIONED BY (dt string)
STORED AS PARQUET
;