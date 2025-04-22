package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Monitor  {


  PgPool client;


  private static final  Map<Long , JsonObject> monitorHashmap = new ConcurrentHashMap<>();

  private static final Map<Long , List<Map<Long,Long>>> monitorAlertMap = new ConcurrentHashMap<>();




  private void registerEventBusConsumers() {


//    registerMonitorCheckPollConsumer();

//    registerRemainingIntervalResetConsumer();

//    registerRemainingIntervalDecrementConsumer();

  }




  private void registerMonitorCheckPollConsumer() {
//    vertx.eventBus().localConsumer(EventBusConstants.EVENT_MONITOR_CHECK_POLL, message -> {
//
//      Long id = (Long) message.body();
//
//      boolean shouldPoll = monitorHashmap.getOrDefault(id, new JsonObject())
//
//        .getLong(VariableConstants.REMAINING_INTERVAL, 0L)
//
//        .equals(0L);
//
//      message.reply(shouldPoll);
//    });
  }

  private void registerRemainingIntervalResetConsumer() {
//    vertx.eventBus().localConsumer(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_RESET, message -> {
//      Long id = (Long) message.body();
//      monitorHashmap.computeIfPresent(id, (key, value) -> {
//        value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.POLLING_INTERVAL, 0L));
//        return value;
//      });
//      message.reply("Interval reset successfully");
//    });
  }

  private void registerRemainingIntervalDecrementConsumer() {
//    vertx.eventBus().localConsumer(EventBusConstants.EVENT_MONITOR_REMAINING_INTERVAL_DECREMENT, message -> {
//      Long id = (Long) message.body();
//      monitorHashmap.computeIfPresent(id, (key, value) -> {
//        value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.REMAINING_INTERVAL, 0L) -10L );
//        System.out.println(value);
//        return value;
//      });
//      message.reply("Interval decremented by 10");
//      System.out.println(monitorHashmap);
//    });
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


}
