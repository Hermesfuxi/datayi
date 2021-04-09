DROP TABLE IF EXISTS dws.user_retention;
CREATE TABLE dws.user_retention
(
    calc_dt    string, -- 计算日期
    first_dt        string,
    retention_days  int,
    retention_users bigint
)
    STORED AS PARQUET;