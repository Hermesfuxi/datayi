drop table if exists dws.user_active_day;
create table dws.user_active_day(
guid    string
)
partitioned by (dt string)
stored as parquet;