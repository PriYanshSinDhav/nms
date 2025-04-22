package com.motadata.polling;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.Map;

import static com.motadata.constants.QueryConstants.ADD_ALERT_SQL;

public class PollingVerticle extends AbstractVerticle {

  private static final String ADD_MONITORING_DATA_SQL = "INSERT INTO NMS_MONITORING(monitorid,value,updatedon) VALUES ($1,$2,CURRENT_TIMESTAMP)";
  PgPool client;

  private ZContext zContext;
  private ZMQ.Socket socket;
  private static Vertx thisvertx;

  @Override
  public void start() throws Exception {


    thisvertx = getVertx();
    client = DatabaseConfig.getDatabaseClient(vertx);
    zContext = new ZContext();
    socket = zContext.createSocket(SocketType.REQ);
    socket.connect("tcp://127.0.0.1:5555");

    vertx.setPeriodic(10L * 1000L, timeHandler -> {
      fetchMonitorMap();
    });


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
//    vertx.eventBus().request(EventBusConstants.EVENT_GET_CREDENTIAL, monitorData.getLong(VariableConstants.CREDENTIAL_ID),
//      handler -> {
//      if (handler.succeeded()) {
////        JsonObject credentialResponse = (JsonObject) handler.result().body();
//
//        JsonObject credentialResponse = CacheStore.getCredentialProfile(monitorData.getLong(VariableConstants.CREDENTIAL_ID));
//        executeMetricFetching(monitorId, monitorData, credentialResponse);
//      }
//    });
  }

  private void executeMetricFetching(Long monitorId, JsonObject monitorData, JsonObject credentialResponse) {
    String username = credentialResponse.getString(VariableConstants.USERNAME);
    String password = credentialResponse.getString(VariableConstants.PASSWORD);
    String ipAddress = monitorData.getString(VariableConstants.IP_ADDRESS);

    vertx.executeBlocking(promise -> {
      String metrics = fetchMetricsFromZMQ(ipAddress, username, password);
      promise.complete(metrics);
    }, result -> {
      if (result.succeeded()) {
        JsonObject jsonObject = new JsonObject(String.valueOf(result.result()));
        System.out.println(jsonObject);
        handleMetricsResponse(monitorId, jsonObject);
      } else {
        System.out.println(result.cause());
      }
    });
  }

  private void handleMetricsResponse(Long monitorId, JsonObject response) {
    System.out.println("Received response: " + response);

    try {

      client.preparedQuery(ADD_MONITORING_DATA_SQL)
        .execute(Tuple.of(monitorId, response))
        .onSuccess(rows -> {
          System.out.println("monitoring done for monitorid " + monitorId);
        })
        .onFailure(err -> System.out.println(err));

      var profiles = CacheStore.getProfilesByMonitor(monitorId);

      for (Long profile : profiles) {
        var profileObject = CacheStore.getProfile(profile);
        var recoredeValue = response.getString(profileObject.getString(VariableConstants.METRIC_VALUE));

        checkAlert(recoredeValue, profileObject, monitorId, profile);

        System.out.println("recorded value " + recoredeValue);

      }


    } catch (Exception e) {
      System.out.println(e);
    }

    CacheStore.resetRemainingInterval(monitorId);
//    vertx.eventBus().request(EventBusConstants. EVENT_MONITOR_REMAINING_INTERVAL_RESET, monitorId).compose(future-> {
//      decrementPollingInterval(monitorId);
//     return Future.succeededFuture();
//    });

  }

  private void checkAlert(String recordedValue, JsonObject profileObject, Long monitorId, Long profileId) {

    var longValue = BigDecimal.valueOf(Double.parseDouble(recordedValue));

//    vertx.eventBus().request(EventBusConstants.CHECK_AND_ADD_ALERT, new JsonObject()
//      .put(VariableConstants.MONITOR_ID, monitorId)
//      .put(VariableConstants.PROFILE_ID, profileId)
//      .put(VariableConstants.PROFILE, profileObject)
//      .put(VariableConstants.VALUE, longValue.longValue()));

    var monitorAlertMap = CacheStore.getAlertForMonitorId(monitorId);
    processAlert(monitorAlertMap, profileId, monitorId, longValue.longValue(), profileObject);


  }

  private void processAlert(Map<Long, JsonObject> monitorAlertMap, Long profileId, Long monitorId, Long value, JsonObject profileObject) {
    var alertJson = createAlertJson(monitorId, profileId, value);

    if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_3)) {
      addOrUpdateAlert(monitorAlertMap, profileId, alertJson, VariableConstants.CRITICAL);
      System.out.println("Critical alert generated");
    } else if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_2)) {
      addOrUpdateAlert(monitorAlertMap, profileId, alertJson, VariableConstants.SEVERE);
    } else if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_1)) {
      addOrUpdateAlert(monitorAlertMap, profileId, alertJson, VariableConstants.WARNING);
    } else {
      clearAlertIfExists(monitorAlertMap, profileId);
    }
    CacheStore.updateAlert(monitorId, monitorAlertMap);
  }

  private void addOrUpdateAlert(Map<Long, JsonObject> monitorAlertMap, Long profileId, JsonObject alertJson, String alertLevel) {
    alertJson.put(VariableConstants.ALERT_LEVEL, alertLevel);
    monitorAlertMap.put(profileId, alertJson);
    insertAlert(alertJson.getLong(VariableConstants.MONITOR_ID), profileId, alertJson.getLong(VariableConstants.VALUE), alertLevel);
  }

  private void insertAlert(Long monitorId, Long profileId, Long value, String level) {

    client.preparedQuery(ADD_ALERT_SQL).execute(Tuple.of(monitorId, profileId, level, value)).onSuccess(rows -> {
      System.out.println("Alert Added ");
    }).onFailure(err -> System.out.println("Error while adding alert reason " + err.getMessage()));
  }

  private void clearAlertIfExists(Map<Long, JsonObject> monitorAlertMap, Long profileId) {
    monitorAlertMap.computeIfPresent(profileId, (key, alert) -> {
      alert.put(VariableConstants.CLEARED, true);
      return alert;
    });
  }


  private JsonObject createAlertJson(Long monitorId, Long profileId, Long value) {
    return new JsonObject()
      .put(VariableConstants.MONITOR_ID, monitorId)
      .put(VariableConstants.PROFILE_ID, profileId)
      .put(VariableConstants.VALUE, value)
      .put(VariableConstants.CLEARED, false);
  }

  private void decrementPollingInterval(Long monitorId) {
    CacheStore.decrementRemainingInterval(monitorId);
//    vertx.eventBus().request(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_DECREMENT, monitorId);
  }


  private String fetchMetricsFromZMQ(String ip, String username, String password) {
    socket.send(new JsonObject().put("ip", ip).put(VariableConstants.USERNAME, username).put(VariableConstants.PASSWORD, password).encode());
    return socket.recvStr();
  }



}


