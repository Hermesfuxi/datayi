DROP TABLE IF EXISTS dws.share_event_overview_bitmap_cube;
CREATE TABLE dws.share_event_overview_bitmap_cube(
   cat_name      string,
   brand_name    string,
   page_id       string,
   lanmu_name    string,
   share_method  string,
   hour_range    int,
   device_type   string,
   user_cnt      bigint,  -- 人数
   share_cnt     bigint   -- 次数
)
PARTITIONED BY (dt  string)
STORED AS parquet
;