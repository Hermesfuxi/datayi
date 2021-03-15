-- 活跃状态bitmap记录表
drop table if exists dws.user_active_status_bitmap;
create table dws.user_active_status_bitmap(
guid        string,
first_dt    string,
active_status  int
)
partitioned by (dt string)
stored as parquet;