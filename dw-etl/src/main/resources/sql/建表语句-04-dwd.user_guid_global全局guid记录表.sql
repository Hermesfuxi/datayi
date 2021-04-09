DROP TABLE IF EXISTS dwd.user_guid_global;
CREATE TABLE IF NOT EXISTS dwd.user_guid_global(
id   bigint,
guid string
)
PARTITIONED BY (dt string)
stored as parquet;