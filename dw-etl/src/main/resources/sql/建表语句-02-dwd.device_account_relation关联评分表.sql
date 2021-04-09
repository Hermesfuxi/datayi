
DROP TABLE IF EXISTS dwd.device_account_relation;
CREATE TABLE dwd.device_account_relation
(
   deviceid   STRING,
   account    STRING,
   score      DOUBLE,
   first_time BIGINT,
   last_time  BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
TBLPROPERTIES('parquet.compress','snappy')
;