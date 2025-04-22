package com.motadata.api;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.polling.PollingVerticle;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import static com.motadata.constants.QueryConstants.*;


public class Device  {


  PgPool client;



  public void init(Router router,PgPool client) {
    this.client = client;
    router.post("/provision").handler(this::provisionDevice);
    router.post("/create").handler(this::createDiscoverDevice);
    router.post("/retry/:id").handler(this::retryDiscoveringDevice);
    router.get("/get").handler(this::getDevicesWithoutPagination);
    router.post("/get").handler(this::getDevicesWithPagination);


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
          .put(VariableConstants.DEVICE_ID,row.getLong(DatabaseConstants.DEVICE_ID))
          .put(VariableConstants.IP_ADDRESS,row.getString(DatabaseConstants.IP_ADDRESS))
          .put(VariableConstants.DEVICE_TYPE_NAME,row.getString(DatabaseConstants.NAME))
          .put(VariableConstants.DEVICE_DISCOVERED,row.getBoolean(DatabaseConstants.DISCOVERED))
          .put(VariableConstants.REMARKS,row.getString(DatabaseConstants.REMARKS))

        ));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,devices));


      }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error occurred while fetching devices reason :-" + err.getMessage())));
  }

  private void getDevicesWithoutPagination(RoutingContext routingContext) {

    client.preparedQuery(GET_ALL_DEVICES).execute()
      .onSuccess(rows -> {

        var devices = new ArrayList<JsonObject>();
        rows.forEach(row -> devices.add(new JsonObject()
          .put(VariableConstants.DEVICE_ID,row.getLong(DatabaseConstants.DEVICE_ID))
          .put(VariableConstants.IP_ADDRESS,row.getString(DatabaseConstants.IP_ADDRESS))
          .put(VariableConstants.DEVICE_TYPE_NAME,row.getString(DatabaseConstants.NAME))
          .put(VariableConstants.DEVICE_DISCOVERED,row.getBoolean(DatabaseConstants.DISCOVERED))
          .put(VariableConstants.REMARKS,row.getString(DatabaseConstants.REMARKS))
          .put(VariableConstants.DEVICE_TYPE_ID,row.getLong(DatabaseConstants.DEVICE_TYPE_ID))

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

    var deviceId = requestBody.getLong(VariableConstants.DEVICE_ID);
    var pollingInterval = requestBody.getLong(VariableConstants.POLLING_INTERVAL);

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

        var credentialId = deviceObject.getValue(DatabaseConstants.CREDENTIAL_ID);
        var ipAddress = deviceObject.getValue(DatabaseConstants.IP_ADDRESS);

        client.preparedQuery(GET_MONITOR_BY_IP_SQL).execute(Tuple.of(ipAddress)).compose(rows->{
          if (rows.iterator().hasNext()) {
            return Future.failedFuture("Monitor already exists with same IP address");
          }
          return Future.succeededFuture();

        }).compose(v-> client.preparedQuery(ADD_MONITOR_SQL).execute(Tuple.of(credentialId,ipAddress,pollingInterval))).onSuccess(rows -> {

            var monitorId = rows.iterator().next().getLong(DatabaseConstants.MONITOR_ID);

            routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,monitorId));


            CacheStore.addMonitor(monitorId,new JsonObject()
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


  private void getAlertDetailsByMetricId(RoutingContext routingContext){

    JsonObject requestBody = routingContext.body().asJsonObject();


    Long pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);

    Long pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);

    Long profileId = requestBody.getLong(VariableConstants.PROFILE_ID);

    String level = requestBody.getString("level");

    String sql = "SELECT * ,n.monitorid as monitorid  FROM NMS_ALERT n join NMS_MONITOR m ON n.monitorid = m.monitorid WHERE  n.profileid = $1 and n.level = $2 ORDER BY timestamp desc   LIMIT $3  OFFSET $4 ";

    client.preparedQuery(sql).execute(Tuple.of(profileId,level,pageSize,pageNumber*pageSize))
      .onSuccess(rows -> {
        var alertDetails = new ArrayList<JsonObject>();

        rows.forEach(row-> {
          alertDetails.add(new JsonObject()
            .put(VariableConstants.ALERT_ID,DatabaseConstants.ALERT_ID)
            .put(VariableConstants.MONITOR_ID,DatabaseConstants.MONITOR_ID)
            .put(VariableConstants.IP_ADDRESS,DatabaseConstants.IP_ADDRESS)
            .put(VariableConstants.ALERT_LEVEL,DatabaseConstants.ALERT_LEVEL)
            .put(VariableConstants.VALUE,DatabaseConstants.VALUE)
            .put(VariableConstants.GENERATED_AT,DatabaseConstants.TIMESTAMP)

          );
        });

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,alertDetails));


      })
      .onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error fetching alert details reason :- " + err.getMessage())));

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


    checkDeviceAccessible(ipAddress,credentialId,deviceTypeId,routingContext);




  }

  private void checkDeviceAccessible(String ipAddress, Long credentialId ,Long deviceTypeId, RoutingContext routingContext) {

    PollingVerticle.pingIPAddress(ipAddress).onComplete(pingFuture-> {
      if (pingFuture.succeeded()) {
        if(pingFuture.result()){

          JsonObject credentialProfile = CacheStore.getCredentialProfile(credentialId);
          if (credentialProfile != null) {

            String username = credentialProfile.getString(VariableConstants.USERNAME);
            String password = credentialProfile.getString(VariableConstants.PASSWORD);


            PollingVerticle.checkLogin(username,ipAddress,password).onComplete(future-> {

              if (future.succeeded() && future.result()) {
                insertDiscoverDevice(credentialId,ipAddress,"Success",true,routingContext,deviceTypeId);
              }else {
                insertDiscoverDevice(credentialId,ipAddress,future.cause().getMessage(),false,routingContext,deviceTypeId);
              }
            });



          }else {
            routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error getting credential Profile"));
          }

        }else {
          insertDiscoverDevice(credentialId,ipAddress,"IP not reachable",false,routingContext,deviceTypeId);
        }
      }else {
        insertDiscoverDevice(credentialId,ipAddress, pingFuture.cause().getMessage(), false,routingContext,deviceTypeId);
      }

    });


  }


  private void retryDiscoveringDevice(RoutingContext routingContext){

    var deviceId = Long.valueOf(routingContext.pathParam("id"));

    String sql = "select * from NMS_DEVICE WHERE deviceid = $1";
    client.preparedQuery(sql).execute(Tuple.of(deviceId)).onComplete(result -> {
      if (result.succeeded()) {

        var row = result.result().iterator().next();

        Long credentialId = row.getLong(DatabaseConstants.CREDENTIAL_ID);
        Long deviceTypeId = row.getLong(DatabaseConstants.DEVICE_TYPE_ID);
        String ipAddress = row.getString(DatabaseConstants.IP_ADDRESS);


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


        checkDeviceAccessible(ipAddress,credentialId,deviceTypeId,routingContext);




      }else {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,result.cause().getMessage()));
        return;
      }
    });


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

