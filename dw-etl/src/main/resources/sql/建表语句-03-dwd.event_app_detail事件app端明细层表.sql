DROP TABLE IF EXISTS dwd.event_app_detail;
CREATE TABLE IF NOT EXISTS dwd.event_app_detail (
    account         String                ,
    appid           String                ,
    appversion      String                ,
    carrier         String                ,
    deviceid        String                ,
    devicetype      String                ,
    eventid         String                ,
    ip              String                ,
    latitude        Double                ,
    longitude       Double                ,
    nettype         String                ,
    osname          String                ,
    osversion       String                ,
    properties      Map<String,String>    ,
    releasechannel  String                ,
    resolution      String                ,
    sessionid       String                ,
    `timestamp`     BIGINT                ,
    newsessionid    String                ,
    location         String                ,
    guid            String                ,
    newflag         String
  )
  PARTITIONED BY (dt string)
  STORED AS parquet
  TBLPROPERTIES("parquet.compress"="snappy");