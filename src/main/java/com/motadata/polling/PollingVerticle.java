package com.motadata.polling;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PollingVerticle extends AbstractVerticle {

  private static final String ADD_MONITORING_DATA_SQL = "INSERT INTO NMS_MONITORING(monitorid,value,updatedon) VALUES ($1,$2,CURRENT_TIMESTAMP)";
  PgPool client;

  private ZContext zContext;
  private ZMQ.Socket socket;

  @Override
  public void start() throws Exception {


    client = DatabaseConfig.getDatabaseClient(vertx);
    zContext = new ZContext();
    socket = zContext.createSocket(SocketType.REQ);
    socket.connect("tcp://127.0.0.1:5555");

    vertx.setPeriodic(10L * 1000L, timeHandler -> {
      fetchMonitorMap();
    });



  }

  private void fetchMonitorMap() {
    vertx.eventBus().request(EventBusConstants.EVENT_MONITOR_MAP_GET, "", reply -> {
      if (reply.succeeded()) {
        JsonObject jsonMap = new JsonObject(reply.result().body().toString());
        processMonitorMap(jsonMap);
      }
    });
  }

  private void processMonitorMap(JsonObject jsonMap) {
    Map<Long, JsonObject> monitorMap = new ConcurrentHashMap<>();
    jsonMap.forEach(entry -> monitorMap.put(Long.valueOf(entry.getKey()), (JsonObject) entry.getValue()));

    monitorMap.forEach(this::handleMonitorEntry);
  }

  private void handleMonitorEntry(Long monitorId, JsonObject monitorData) {
    vertx.eventBus().request(EventBusConstants.EVENT_MONITOR_CHECK_POLL, monitorId, check -> {
      if (check.succeeded() && (boolean) check.result().body()) {
        processPolling(monitorId, monitorData);
      } else {
        decrementPollingInterval(monitorId);
      }
    });
  }

  private void processPolling(Long monitorId, JsonObject monitorData) {
    fetchCredential(monitorId, monitorData);
  }

  private void fetchCredential(Long monitorId, JsonObject monitorData) {
    vertx.eventBus().request(EventBusConstants.EVENT_GET_CREDENTIAL, monitorData.getLong(VariableConstants.CREDENTIAL_ID),
      handler -> {
      if (handler.succeeded()) {
        JsonObject credentialResponse = (JsonObject) handler.result().body();
        executeMetricFetching(monitorId, monitorData, credentialResponse);
      }
    });
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
      }else {
        System.out.println(result.cause());
      }
    });
  }

  private void handleMetricsResponse(Long monitorId, JsonObject response) {
    System.out.println("Received response: " + response);

    try {

      client.preparedQuery( ADD_MONITORING_DATA_SQL )
        .execute(Tuple.of(monitorId,response))
        .onSuccess(rows -> {
          System.out.println("monitoring done for monitorid " + monitorId);
        })
        .onFailure(err-> System.out.println(err));

      vertx.eventBus().request(EventBusConstants.GET_PROFILES_FOR_MONITOR,monitorId).onSuccess(objectMessage -> {
        if(objectMessage.body() != null){
          var profiles = (List<Long>) objectMessage.body();

          for (Long profile : profiles) {
            vertx.eventBus().request(EventBusConstants.EVENT_GET_PROFILE,profile).onSuccess(message -> {
              var profileObject = (JsonObject) message.body();
              var recoredeValue = response.getString(profileObject.getString(VariableConstants.METRIC_VALUE));

              checkAlert(recoredeValue,profileObject,monitorId,profile);

              System.out.println("recorded value " + recoredeValue);
            });
          }

        }

      }).onFailure(err-> System.out.println(err));

    } catch (Exception e) {
      System.out.println(e);
    }

    vertx.eventBus().request(EventBusConstants. EVENT_MONITOR_REMAINING_INTERVAL_RESET, monitorId).compose(future-> {
      decrementPollingInterval(monitorId);
     return Future.succeededFuture();
    });

  }

  private void checkAlert(String recordedValue, JsonObject profileObject,Long monitorId,Long profileId) {

    var longValue = BigDecimal.valueOf(Double.parseDouble(recordedValue));

    vertx.eventBus().request(EventBusConstants.CHECK_AND_ADD_ALERT,new JsonObject()
      .put(VariableConstants.MONITOR_ID,monitorId)
      .put(VariableConstants.PROFILE_ID,profileId)
      .put(VariableConstants.PROFILE,profileObject)
      .put(VariableConstants.VALUE,longValue.longValue()));


  }

  private void decrementPollingInterval(Long monitorId) {
    vertx.eventBus().request(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_DECREMENT, monitorId);
  }


  private String fetchMetricsFromZMQ(String ip, String username ,String password) {
    socket.send(new JsonObject().put("ip",ip).put(VariableConstants.USERNAME,username).put(VariableConstants.PASSWORD,password).encode());
//    System.out.println("socket output " + socket.recvStr());
    return socket.recvStr();
  }



}
