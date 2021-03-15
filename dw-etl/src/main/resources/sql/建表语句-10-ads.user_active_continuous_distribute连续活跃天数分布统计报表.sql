-- 最近一个月连续活跃天数分布报表
create table ads.user_active_continuous_distribute(
calc_dt     string,
days_1_10   bigint,
days_10_20  bigint,
days_20     bigint
)
stored as parquet;