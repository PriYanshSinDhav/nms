package com.motadata.api;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.nms.MainVerticle;
import com.motadata.utility.*;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import java.net.InetAddress;
import java.util.ArrayList;
import static com.motadata.constants.QueryConstants.*;

public class Device  {


  private PgPool client;



  public void init(Router router) {
    this.client = DatabaseConfig.getDatabaseClient();
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
        routingContext.json( JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, err.getMessage()));
      }
    );

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

    pingIPAddress(ipAddress).onComplete(pingFuture-> {
      if (pingFuture.succeeded()) {
        if(pingFuture.result()){

          JsonObject credentialProfile = CacheStore.getCredentialProfile(credentialId);
          if (credentialProfile != null) {

            String username = credentialProfile.getString(VariableConstants.USERNAME);
            String password = credentialProfile.getString(VariableConstants.PASSWORD);


            checkLogin(username,ipAddress,password).onComplete(future-> {

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

    var deviceId = Long.valueOf(routingContext.pathParam(VariableConstants.ID));

    client.preparedQuery(GET_DEVICE_BY_ID_SQL).execute(Tuple.of(deviceId)).onComplete(result -> {
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

    client.preparedQuery(INSERT_DEVICE)
      .execute(Tuple.of(credentialId, ipAddress, remarks, discovered,deviceTypeId))
      .onSuccess(res -> {
        routingContext.json(new JsonObject().put("message", "Device discovery logged").put("remarks", remarks).put("discovered", discovered));

      })
      .onFailure(err -> routingContext.fail(500, err));
  }


  private  Future<Boolean> pingIPAddress(String ipAdress) {
      var vertx = MainVerticle.getVertx();


    Promise<Boolean> resultPromise = Promise.promise();
    vertx.executeBlocking(promise -> {
      try {
        InetAddress inetAddress = InetAddress.getByName(ipAdress);
        boolean reachable = inetAddress.isReachable(3000);
        resultPromise.complete(reachable);
      } catch (Exception e) {
        System.out.println(e);
        resultPromise.fail(e);
      }
    }, resultPromise);

    return resultPromise.future();
  }

  private Future<Boolean> checkLogin(String username, String ip, String password) {
    Promise<Boolean> resultPromise = Promise.promise();

    var vertx = MainVerticle.getVertx();

    vertx.executeBlocking(promise -> {

      try {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, ip, 22);
        session.setPassword(password);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(3000);
        session.disconnect();
        promise.complete(true);
      } catch (Exception e) {
        System.out.println(e);
        promise.fail(e);
      }
    }, resultPromise);

    return resultPromise.future();

  }

}

