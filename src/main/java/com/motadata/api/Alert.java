package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Alert extends AbstractVerticle {

  private static final String ADD_ALERT_SQL = "INSERT INTO NMS_ALERT (monitorid,profileid,level,value) values ($1,$2,$3,$4)";
  private final Router router;

  private static final Map<Long, Map<Long,JsonObject>> ALERT_CACHE_MAP = new ConcurrentHashMap();

  PgPool client ;

  public Alert(Router router) {
    this.router = router;
  }

  @Override
  public void start() throws Exception {

    client = DatabaseConfig.getDatabaseClient(vertx);


    vertx.eventBus().consumer(EventBusConstants.CHECK_AND_ADD_ALERT, message -> {

      var jsonObject = (JsonObject) message.body();
      var profileId = jsonObject.getLong(VariableConstants.PROFILE_ID);
      var monitorId = jsonObject.getLong(VariableConstants.MONITOR_ID);
      var value = jsonObject.getLong(VariableConstants.VALUE);
      var profileObject = jsonObject.getJsonObject(VariableConstants.PROFILE);

      var monitorAlertMap = ALERT_CACHE_MAP.computeIfAbsent(monitorId, key -> new ConcurrentHashMap<>());

      processAlert(monitorAlertMap, profileId, monitorId, value, profileObject);
    });

//    router.get().handler(this::insertAlert);

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
  }

  private void addOrUpdateAlert(Map<Long, JsonObject> monitorAlertMap, Long profileId, JsonObject alertJson, String alertLevel) {
    alertJson.put(VariableConstants.ALERT_LEVEL, alertLevel);
    monitorAlertMap.put(profileId, alertJson);
    insertAlert(alertJson.getLong(VariableConstants.MONITOR_ID),profileId,alertJson.getLong(VariableConstants.VALUE),alertLevel);
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


  private void insertAlert(Long monitorId, Long profileId, Long value, String level) {

    client.preparedQuery(ADD_ALERT_SQL).execute(Tuple.of(monitorId, profileId, level, value)).onSuccess(rows -> {
      System.out.println("Alert Added ");
    }).onFailure(err -> System.out.println("Error while adding alert reason " + err.getMessage()));
  }

  private void checkAlert(){

  }
}
