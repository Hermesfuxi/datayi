DROP TABLE IF EXISTS dws.share_event_overview_bitmap;
CREATE TABLE dws.share_event_overview_bitmap(
   cat_name      string,
   brand_name    string,
   page_id       string,
   lanmu_name    string,
   share_method  string,
   hour_range    int,
   device_type   string,
   guid_bitmap   binary,   -- 一组中的guid的bitmap记录
   share_cnt     bigint
)
PARTITIONED BY (dt  string)
STORED AS parquet
;
