drop table if exists dws.app_flow_overview_bitmap;
create table dws.app_flow_overview_bitmap(
    location              string,
    hour_range            string   ,
    device_type           string,
    os_name               string,
    start_page            string,
    end_page              string,
    isnew                 string,
    pv_cnt                bigint,
    uv_bitmap             binary,
    session_cnt           bigint,
    time_long             bigint,
    return_user_bitmap    binary,
    jumpout_cnt           bigint,
    jumpout_user_bitmap   binary
)
PARTITIONED BY (dt string)
STORED AS PARQUET;