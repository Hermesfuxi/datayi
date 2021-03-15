drop table if exists dws.user_active_time_range;
create table dws.user_active_time_range(
  guid       string,
  first_dt   string,
  range_start  string,
  range_end    string
)
PARTITIONED BY (dt string)
STORED AS parquet;
