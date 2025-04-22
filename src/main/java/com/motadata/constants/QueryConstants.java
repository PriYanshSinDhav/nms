package com.motadata.constants;

public class QueryConstants {

  public static final String QUERY_SELECT_ALL_CREDENTIALS = "SELECT * FROM NMS_CREDENTIALS";

  public static final String QUERY_SELECT_CREDENTIAL_BY_ID = "SELECT * FROM NMS_CREDENTIALS WHERE id = $1";

  public static final String QUERY_INSERT_CREDENTIAL = "INSERT INTO NMS_CREDENTIALS(USERNAME,PASSWORD) VALUES ($1,$2) RETURNING id";

  public static final String QUERY_UPDATE_CREDENTIAL = "UPDATE NMS_CREDENTIALS SET USERNAME = $2 , PASSWORD = $3 WHERE id = $1 RETURNING id";

  public static final String GET_CREDENTIAL_PAGE_SQL = "SELECT * FROM NMS_CREDENTIALS LIMIT $1 OFFSET $2";

  public static final String ADD_ALERT_SQL = "INSERT INTO NMS_ALERT (monitorid,profileid,level,value) values ($1,$2,$3,$4)";

  public static final String GET_PROFILES_SQL = "select P.PROFILEID AS PROFILEID,P.METRICID AS METRICID,  P.ALERTLEVEL1 AS ALERTLEVEL1, P.ALERTLEVEL2 AS ALERTLEVEL2, P.ALERTLEVEL3 AS ALERTLEVEL3,P.NAME AS NAME, M.METRICVALUE AS METRICVALUE   from NMS_PROFILE P join NMS_METRIC M ON P.metricid = M.metricid";
  public static final String GET_PROFILE_PAGINATION_SQL = "select * from NMS_PROFILE limit $1 offset $2";
  public static final String CREATE_PROFILE_SQL = "INSERT INTO NMS_PROFILE (metricid,name,alertlevel1,alertlevel2,alertlevel3,metricvalue) VALUES ($1,$2,$3,$4,$5,$6) returning profileid";

  public static final String QUERY_GET_ALL_MONITORS = "SELECT * FROM NMS_MONITOR";

  public static final String ADD_METRIC_SQL = "INSERT INTO NMS_METRIC (name , devicetypeid ,alertable , metricvalue) values ($1,$2,$3,$4) returning metricid";
  public static final String GET_METRIC_SQL = "SELECT * FROM NMS_METRIC";

  public static final String ADD_MONITOR_PROFILE_REL_SQL = "INSERT INTO nms_monitor_profile (monitorid , profileid) values ($1,$2) ";
  public static final String GET_MONITOR_PROFILE_REL_SQL = "select * from nms_monitor_profile " ;
  public static final String GET_ALL_PROFILES_BY_MONITOR_SQL = "select p.name as profilename from nms_monitor_profile mp join nms_profile p on mp.profileid = p.profileid where mp.monitorid = $1";
  public static final String GET_ALL_MONITORS_BY_PROFILE_SQL = "select p.ipaddress as ipaddress  from nms_monitor_profile mp join nms_monitor m on mp.monitorid = m.monitorid where mp.profileid = $1";

  public static final String ADD_MONITOR_SQL = "INSERT INTO NMS_MONITOR (credentialid,ipaddress,pollinginterval, createdon) VALUES ($1,$2,$3,CURRENT_TIMESTAMP) returning monitorid";

  public static final String GET_DEVICE_BY_ID_SQL = "select * from NMS_DEVICE WHERE deviceid = $1";
  public static final String GET_MONITOR_BY_IP_SQL = "select * from NMS_MONITOR where ipaddress = $1";
  public static final String GET_ALL_DEVICES = "SELECT d.deviceid , d.ipaddress ,dt.name , d.discovered , d.remarks , d.devicetypeid FROM NMS_DEVICE d join ncm_devicetype dt on d.devicetypeid = dt.devicetypeid ";


  public static final String GET_ALL_DEVICES_PAGINATION =  "SELECT d.deviceid , d.ipaddress ,dt.name , d.discovered , d.remarks FROM NMS_DEVICE d join ncm_devicetype dt on d.devicetypeid = dt.devicetypeid WHERE (1 = $1 OR d.discovered = $2 ) LIMIT $3 OFFSET $4";

  public static final String GET_ALL_ALERTS_BY_MONITOR= "SELECT * , n.profileid as profileid , p.name as profilename FROM NMS_ALERT n JOIN NMS_PROFILE p on n.profileid = p.profileid  WHERE  n.monitorid = $1 and n.level = $2 ORDER BY timestamp desc  LIMIT $3  OFFSET $4";

  public static final String GET_ALL_ALERTS_BY_PROFILE = "SELECT n.alertid , n.monitorid , n.profileid , n.level , n.timestamp ,n.value , p.name as profilename, m.ipaddress   FROM NMS_ALERT n join NMS_MONITOR m ON n.monitorid = m.monitorid join NMS_PROFILE p on n.profileid = p.profileid  WHERE  (1 = $5 OR n.profileid = $1)  and (1 = $6 or n.level = $2)  ORDER BY timestamp desc LIMIT $3  OFFSET $4 ";



  public static final String INSERT_DEVICE  = "INSERT INTO NMS_DEVICE (credentialid, ipaddress, remarks, discovered,devicetypeid) VALUES ($1, $2, $3, $4,$5)";

}
