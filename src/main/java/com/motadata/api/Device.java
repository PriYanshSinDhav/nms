package com.motadata.api;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.net.InetAddress;
import java.util.ArrayList;


public class Device extends AbstractVerticle {

  private static final String GET_DEVICE_BY_ID_SQL = "select * from NMS_DEVICE WHERE deviceid = $1";
  private static final String GET_MONITOR_BY_IP_SQL = "select * from NMS_MONITOR where ipaddress = $1";
  private static final String GET_ALL_DEVICES = "SELECT d.deviceid , d.ipaddress ,dt.name , d.discovered , d.remarks FROM NMS_DEVICE d join ncm_devicetype dt on d.devicetypeid = dt.devicetypeid ";


  private static final String GET_ALL_DEVICES_PAGINATION =  "SELECT d.deviceid , d.ipaddress ,dt.name , d.discovered , d.remarks FROM NMS_DEVICE d join ncm_devicetype dt on d.devicetypeid = dt.devicetypeid WHERE (1 = $1 OR d.discovered = $2 ) LIMIT $3 OFFSET $4";

  private final Router router;
  PgPool client;


  private static final String CREDENTIAL_ID = "credentialid";
  private static final String IP_ADDRESS = "ipaddress";
  private static final String MONITOR_ID = "monitorid";
  private static final String DISCOVERED = "discovered";

  private static final String DEVICE_ID = "deviceid";

  private static final String NAME = "name";
  private static final String REMARKS = "remarks";


  private static final String ADD_MONITOR_SQL = "INSERT INTO NMS_MONITOR (credentialid,ipaddress,pollinginterval, createdon) VALUES ($1,$2,$3,CURRENT_TIMESTAMP) returning monitorid";





  public Device(Router router) {
    this.router = router;
  }

  @Override
  public void start() throws Exception {

    this.client = DatabaseConfig.getDatabaseClient(vertx);

    router.post("/device/provision").handler(this::provisionDevice);
    router.post("/create/device").handler(this::createDiscoverDevice);
    router.get("/devices").handler(this::getDevicesWithoutPagination);
    router.post("/devices").handler(this::getDevicesWithPagination);


//    router.getRoutes().forEach(route ->
//      System.out.println("Configured Route: " + route.getPath())
//    );


  }

  private void getDevicesWithPagination(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();


    if(requestBody == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Pagination Data required for getting devices")) ;
      return;
    }

    var pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);
    var pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);

    if(pageSize == null || pageNumber == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Pagination Data required for getting devices")) ;
      return;
    }
    var discovered = requestBody.getBoolean(VariableConstants.DEVICE_DISCOVERED);


    var discoverPresent = 1L;
    if (discovered!=null) {
      discoverPresent = 0L;
    }

    client.preparedQuery(GET_ALL_DEVICES_PAGINATION)
      .execute(Tuple.of(discoverPresent,discovered,pageSize,pageNumber*pageSize)).onSuccess(rows -> {

        var devices = new ArrayList<JsonObject>();
        rows.forEach(row -> devices.add(new JsonObject()
          .put(VariableConstants.DEVICE_ID,row.getLong(DEVICE_ID))
          .put(VariableConstants.IP_ADDRESS,row.getString(IP_ADDRESS))
          .put(VariableConstants.DEVICE_TYPE_NAME,row.getString(NAME))
          .put(VariableConstants.DEVICE_DISCOVERED,row.getBoolean(DISCOVERED))
          .put(VariableConstants.REMARKS,row.getString(REMARKS))

        ));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,devices));


      }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error occurred while fetching devices reason :-" + err.getMessage())));
  }

  private void getDevicesWithoutPagination(RoutingContext routingContext) {

    client.preparedQuery(GET_ALL_DEVICES).execute()
      .onSuccess(rows -> {

        var devices = new ArrayList<JsonObject>();
        rows.forEach(row -> devices.add(new JsonObject()
          .put(VariableConstants.DEVICE_ID,row.getLong(DEVICE_ID))
          .put(VariableConstants.IP_ADDRESS,row.getString(IP_ADDRESS))
          .put(VariableConstants.DEVICE_TYPE_NAME,row.getString(NAME))
          .put(VariableConstants.DEVICE_DISCOVERED,row.getBoolean(DISCOVERED))
          .put(VariableConstants.REMARKS,row.getString(REMARKS))

        ));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,devices));


      }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error occurred while fetching devices reason :-" + err.getMessage())));
  }


  private void provisionDevice(RoutingContext routingContext) {

    System.out.println("inside provision device ");
    var requestBody =  routingContext.body().asJsonObject();

    if(requestBody == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Data required to provision device"));
      return;
    }

    var deviceId = requestBody.getLong("deviceId");
    var pollingInterval = requestBody.getLong("pollingInterval");

    if(deviceId == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Please select device to provision device"));
      return;
    } else if (pollingInterval == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Polling interval required to provision device"));
      return;
    }


    client.preparedQuery(GET_DEVICE_BY_ID_SQL).execute(Tuple.of(deviceId)).onSuccess(result -> {
      if (result.iterator().hasNext()) {
        var deviceObject = result.iterator().next();

        var credentialId = deviceObject.getValue(CREDENTIAL_ID);
        var ipAddress = deviceObject.getValue(IP_ADDRESS);

        client.preparedQuery(GET_MONITOR_BY_IP_SQL).execute(Tuple.of(ipAddress)).compose(rows->{
          if (rows.iterator().hasNext()) {
            return Future.failedFuture("Monitor already exists with same IP address");
          }
          return Future.succeededFuture();

        }).compose(v-> client.preparedQuery(ADD_MONITOR_SQL).execute(Tuple.of(credentialId,ipAddress,pollingInterval))).onSuccess(rows -> {

            var monitorId = rows.iterator().next().getValue(MONITOR_ID);

            routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,monitorId));

            vertx.eventBus().request(EventBusConstants.EVENT_MONITOR_MAP_ADD,new JsonObject()
              .put(VariableConstants.MONITOR_ID,monitorId)
              .put(VariableConstants.POLLING_INTERVAL,pollingInterval)
              .put(VariableConstants.REMAINING_INTERVAL,pollingInterval)
              .put(VariableConstants.IP_ADDRESS,ipAddress)
              .put(VariableConstants.CREDENTIAL_ID,credentialId));

          })
          .onFailure(err->{
            routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Monitor already exists with same IP address"));
            return;
          });
      }else {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Device not found to monitor"));
      }

    }).onFailure(
      err -> {
        routingContext.fail(500 , err);
      }
    );

  }


  private void getAlertDetails(RoutingContext routingContext){

    JsonObject requestBody = routingContext.body().asJsonObject();


    Long pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);

    Long pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);

      Long metricId = requestBody.getLong(VariableConstants.METRIC_ID);

    Long level = requestBody.getLong("level");

    String sql = "SELECT * FROM NMS_ALERT n WHERE  n.metricid = $1 and n.cleared = false and n.level = $2   LIMIT $3  OFFSET $4";

    client.preparedQuery(sql).execute(Tuple.of(metricId,level,pageSize,pageNumber*pageSize));

  }

  private void getMonitoringDetails(RoutingContext routingContext){


    JsonObject requestBody = routingContext.body().asJsonObject();

    Long metricId = requestBody.getLong(VariableConstants.METRIC_ID);

//    Long


    String sql = "";
//    client.preparedQuery(sql,Tuple.of())

  }

  private void createDiscoverDevice(RoutingContext routingContext) {

    System.out.println("inside createDiscoverDevice ");
    JsonObject requestBody = routingContext.body().asJsonObject();

    if (requestBody == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Data required to insert device"));
      return;
    }

    Long credentialId = requestBody.getLong(VariableConstants.CREDENTIAL_ID);
    Long deviceTypeId = requestBody.getLong(VariableConstants.DEVICE_TYPE_ID);
    String ipAddress = requestBody.getString(VariableConstants.IP_ADDRESS);

    if(credentialId == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Credential is required to insert device"));
      return;
    } else if (deviceTypeId == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Device type  is required to insert device"));
      return;
    } else if (ipAddress == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"IP Address  is required to insert device"));
      return;
    }


    vertx.executeBlocking(promise -> {
       pingIPAddress(ipAddress,promise);

    }).onSuccess(reachable -> {

      if((boolean)reachable){
        vertx.eventBus().<JsonObject>request(EventBusConstants.EVENT_GET_CREDENTIAL_PROFILE,credentialId, reply->{
          if (reply.succeeded()) {
            JsonObject credentialProfile =  reply.result().body();

            String username = credentialProfile.getString(VariableConstants.USERNAME);
            String password = credentialProfile.getString(VariableConstants.PASSWORD);

            vertx.executeBlocking(promise -> {

               checkLogin(ipAddress,username,password,promise);

            }).onSuccess(accessible -> {
                insertDiscoverDevice(credentialId,ipAddress,"Success",true,routingContext,deviceTypeId);


              })
              .onFailure(notAccessible -> {
                insertDiscoverDevice(credentialId,ipAddress,notAccessible.getMessage(),false,routingContext,deviceTypeId);
              });

          }else {

            routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error getting credential Profile reason -> " + reply.cause()));
          }
        });
      }else {
        insertDiscoverDevice(credentialId,ipAddress,"IP not reachable",false,routingContext,deviceTypeId);
      }
    }).onFailure(notReachable -> {
      insertDiscoverDevice(credentialId,ipAddress, notReachable.getMessage(), false,routingContext,deviceTypeId);
    });


  }



  private void pingIPAddress(String ipAdress,Promise<Object> promise){
    try {
      InetAddress inetAddress = InetAddress.getByName(ipAdress);
      boolean reachable =  inetAddress.isReachable(3000);
      promise.complete(reachable);
    } catch (Exception e) {
      promise.fail(e);
    }

  }

  private void checkLogin(String ip,String username, String password,Promise<Object> promise){

    JSch jsch = new JSch();

    try{
      Session session = jsch.getSession(username,ip,22);
      session.setPassword(password);
      session.setConfig("StrictHostKeyChecking","no");
      session.connect(3000);

      session.disconnect();
      promise.complete(true);
    } catch (Exception e) {
      System.out.println(e);
      promise.fail(e);
    }

  }


  private void insertDiscoverDevice(Long credentialId, String ipAddress, String remarks, boolean discovered, RoutingContext routingContext,Long deviceTypeId) {
    String sql = "INSERT INTO NMS_DEVICE (credentialid, ipaddress, remarks, discovered,devicetypeid) VALUES ($1, $2, $3, $4,$5)";

    client.preparedQuery(sql)
      .execute(Tuple.of(credentialId, ipAddress, remarks, discovered,deviceTypeId))
      .onSuccess(res -> {
        routingContext.json(new JsonObject().put("message", "Device discovery logged").put("remarks", remarks).put("discovered", discovered));

      })
      .onFailure(err -> routingContext.fail(500, err));
  }



}

