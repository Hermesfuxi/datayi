DROP TABLE IF EXISTS dws.app_flow_agg_session;
CREATE TABLE dws.app_flow_agg_session(
   guid         string,
   session_id   string,
   start_time   string,
   end_time     string,
   start_page   string,
   end_page     string,
   pv_cnt       int,
   isnew        string,
   hour_range   String,
   location     string,
   device_type  string,
   os_name      string,
   release_ch   string
)
partitioned by (dt string)
stored as parquet;