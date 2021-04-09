drop table if exists ads.user_active_silent_days;
create table ads.user_active_silent_days(
calc_dt   string,
slient_3day   bigint,
slient_5day   bigint
)
stored as parquet
;

