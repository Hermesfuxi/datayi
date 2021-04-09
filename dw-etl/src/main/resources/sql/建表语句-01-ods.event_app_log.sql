DROP TABLE IF EXISTS `ods.event_app_log`;
CREATE EXTERNAL TABLE `ods.event_app_log`(         
  `account` string ,    
  `appid` string ,      
  `appversion` string , 
  `carrier` string ,    
  `deviceid` string ,   
  `devicetype` string , 
  `eventid` string ,    
  `ip` string ,         
  `latitude` double ,   
  `longitude` double ,  
  `nettype` string ,    
  `osname` string ,     
  `osversion` string ,  
  `properties` map<string,string> ,  
  `releasechannel` string ,   
  `resolution` string ,   
  `sessionid` string ,   
  `timestamp` bigint 
)   
PARTITIONED BY (`dt` string)                                      
ROW FORMAT SERDE                                    
  'org.apache.hive.hcatalog.data.JsonSerDe'         
STORED AS INPUTFORMAT                               
  'org.apache.hadoop.mapred.TextInputFormat'        
OUTPUTFORMAT                                        
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
LOCATION                                            
  'hdfs://hdp01.doitedu.cn:8020/user/hive/warehouse/ods.db/event_app_log'  
TBLPROPERTIES (                                     
  'bucketing_version'='2',                          
  'transient_lastDdlTime'='1611156713'
);  