package com.motadata.polling;

import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.Map;

public class PollingVerticle extends AbstractVerticle {


  PgPool client;

  private ZContext zContext;
  private ZMQ.Socket senderSocket;
  private ZMQ.Socket  recieverSocket;

  @Override
  public void start() throws Exception {


    client = DatabaseConfig.getDatabaseClient(vertx);
    zContext = new ZContext();
    senderSocket = zContext.createSocket(SocketType.PUSH);
    senderSocket.connect("tcp://127.0.0.1:5555");

    recieverSocket = zContext.createSocket(SocketType.PULL);
    recieverSocket.connect("tcp://127.0.0.1:5556");

    vertx.setPeriodic(10L * 1000L, timeHandler -> {
      fetchMonitorMap();
    });

    getMetrics();

  }

  private void getMetrics(){

    new Thread(() ->
    {
      while (true)
      {
        try
        {
          var bytes = recieverSocket.recv();

          if (bytes != null && bytes.length > 0)
          {
            String metrics = new String(bytes);

            System.out.println(metrics);
            JsonObject jsonObject = new JsonObject(metrics);

            jsonObject.mergeIn(new JsonObject(jsonObject.getString("output"))).remove("output");

            vertx.eventBus().send(EventBusConstants.ADD_METRIC_DETAILS,jsonObject);
            vertx.eventBus().send(EventBusConstants.CHECK_AND_ADD_ALERT,jsonObject);

          }
        }
        catch (Exception exception)
        {
          System.out.println(exception);
        }
      }



    }, "").start();

  }

  private void fetchMonitorMap() {

    System.out.println("in fetch monitor map");
    Map<Long, JsonObject> monitorMap = CacheStore.getAllMonitors();
    monitorMap.forEach(this::handleMonitorEntry);

  }


  private void handleMonitorEntry(Long monitorId, JsonObject monitorData) {
    if (CacheStore.shouldPoll(monitorId)) {
      processPolling(monitorId, monitorData);
    } else {
      decrementPollingInterval(monitorId);
    }
  }

  private void processPolling(Long monitorId, JsonObject monitorData) {
    fetchCredential(monitorId, monitorData);
  }

  private void fetchCredential(Long monitorId, JsonObject monitorData) {
    JsonObject credentialResponse = CacheStore.getCredentialProfile(monitorData.getLong(VariableConstants.CREDENTIAL_ID));
    executeMetricFetching(monitorId, monitorData, credentialResponse);

  }




  private void executeMetricFetching(Long monitorId, JsonObject monitorData, JsonObject credentialResponse) {
    String username = credentialResponse.getString(VariableConstants.USERNAME);
    String password = credentialResponse.getString(VariableConstants.PASSWORD);
    String ipAddress = monitorData.getString(VariableConstants.IP_ADDRESS);

    fetchMetricsFromZMQ(ipAddress, username, password,monitorId);
//    vertx.executeBlocking(promise -> {
//      String metrics = fetchMetricsFromZMQ(ipAddress, username, password,monitorId);
//      promise.complete(metrics);
//    }, result -> {
//      if (result.succeeded()) {
//        JsonObject jsonObject = new JsonObject(String.valueOf(result.result()));
//        System.out.println(jsonObject);
//        var json = new JsonObject().put(VariableConstants.MONITOR_ID,monitorId).mergeIn(jsonObject);
//        vertx.eventBus().send(EventBusConstants.ADD_METRIC_DETAILS,json);
//        vertx.eventBus().send(EventBusConstants.CHECK_AND_ADD_ALERT,json);
////        handleMetricsResponse(monitorId, jsonObject);
//      } else {
//        System.out.println(result.cause());
//      }
//    });
  }



  private void decrementPollingInterval(Long monitorId) {
    CacheStore.decrementRemainingInterval(monitorId);
  }


  private void fetchMetricsFromZMQ(String ip, String username, String password,Long monitorId) {
    senderSocket.send(new JsonObject().put(VariableConstants.MONITOR_ID,String.valueOf(monitorId)).put("ip", ip).put(VariableConstants.USERNAME, username).put(VariableConstants.PASSWORD, password).encode());
//    return senderSocket.recvStr();
  }



}


