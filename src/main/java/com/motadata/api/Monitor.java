package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.JsonObjectUtility;
import com.motadata.utility.ResponseConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Monitor extends AbstractVerticle {

  private final Router router;

  PgPool client;

  private static final String QUERY_GET_ALL_MONITORS = "SELECT * FROM NMS_MONITOR";

  private static final String CREDENTIAL_ID = "credentialid";

  private static final String IP_ADDRESS = "ipaddress";

  private static final String POLLING_INTERVAL = "pollinginterval";

  private static final String MONITOR_ID = "monitorid";

  private static final  Map<Long , JsonObject> monitorHashmap = new ConcurrentHashMap<>();

  private static final Map<Long , List<Map<Long,Long>>> monitorAlertMap = new ConcurrentHashMap<>();

  public Monitor(Router router) {
    this.router = router;
  }


  @Override
  public void start() throws Exception {

    client = DatabaseConfig.getDatabaseClient(vertx);

    client.preparedQuery(QUERY_GET_ALL_MONITORS).execute().onSuccess(res -> res.forEach(Monitor::accept));

    registerEventBusConsumers();

    router.get().handler(this::getDevicesForAlertMonitoring);

  }

  private void registerEventBusConsumers() {
    registerMonitorMapGetConsumer();

    registerRemainingIntervalGetConsumer();

    registerMonitorCheckPollConsumer();

    registerRemainingIntervalResetConsumer();

    registerRemainingIntervalDecrementConsumer();

    registerMonitorMapAddConsumer();
  }

  private void registerMonitorMapGetConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_MAP_GET, message -> {

      JsonObject jsonMap = new JsonObject();

      monitorHashmap.forEach((key, value) -> jsonMap.put(String.valueOf(key), value));

      message.reply(jsonMap);

    });
  }

  private void registerRemainingIntervalGetConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_GET, message -> {
      Long id = (Long) message.body();
      Long remainingInterval = monitorHashmap.getOrDefault(id, new JsonObject()).getLong(VariableConstants.REMAINING_INTERVAL, 0L);
      message.reply(remainingInterval);
    });
  }

  private void registerMonitorCheckPollConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_CHECK_POLL, message -> {

      Long id = (Long) message.body();

      boolean shouldPoll = monitorHashmap.getOrDefault(id, new JsonObject())

        .getLong(VariableConstants.REMAINING_INTERVAL, 0L)

        .equals(0L);

      message.reply(shouldPoll);
    });
  }

  private void registerRemainingIntervalResetConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_RESET, message -> {
      Long id = (Long) message.body();
      monitorHashmap.computeIfPresent(id, (key, value) -> {
        value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.POLLING_INTERVAL, 0L));
        return value;
      });
      message.reply("Interval reset successfully");
    });
  }

  private void registerRemainingIntervalDecrementConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_DECREMENT, message -> {
      Long id = (Long) message.body();
      monitorHashmap.computeIfPresent(id, (key, value) -> {
        value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.REMAINING_INTERVAL, 0L) -10L );
        System.out.println(value);
        return value;
      });
      message.reply("Interval decremented by 10");
      System.out.println(monitorHashmap);
    });
  }

  private void registerMonitorMapAddConsumer() {
    vertx.eventBus().consumer(EventBusConstants.EVENT_MONITOR_MAP_ADD, message -> {
      var jsonObject = (JsonObject) message.body();
      monitorHashmap.put(jsonObject.getLong(VariableConstants.MONITOR_ID), jsonObject);
    });
  }


  private void getDevicesForAlertMonitoring(RoutingContext routingContext) {

    String sql = "SELECT * FROM NMS_MONITORS WHERE MONITORING = true ";
    client.preparedQuery(sql).execute().onSuccess(res-> {

      List<JsonObject> monitors = new ArrayList<>();
      for (Row re : res) {
        monitors.add(new JsonObject().put("",re.getValue("")));
      }
      JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,monitors);
    }).onFailure(err -> JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG));



  }
  private static void accept(Row r) {
    monitorHashmap.put(r.getLong(MONITOR_ID),
      new JsonObject()
        .put(VariableConstants.CREDENTIAL_ID, r.getValue(CREDENTIAL_ID))
        .put(VariableConstants.IP_ADDRESS, r.getValue(IP_ADDRESS))
        .put(VariableConstants.POLLING_INTERVAL, r.getValue(POLLING_INTERVAL))
        .put(VariableConstants.REMAINING_INTERVAL, r.getValue(POLLING_INTERVAL))
    );
  }

}
