package com.motadata.api;

import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.math.BigDecimal;
import java.util.Map;

import static com.motadata.constants.QueryConstants.ADD_ALERT_SQL;

public class AlertVerticle extends AbstractVerticle {

  PgPool client ;

  @Override
  public void start() {

    client = DatabaseConfig.getDatabaseClient(vertx);
    vertx.eventBus().consumer(EventBusConstants.CHECK_AND_ADD_ALERT, message -> {


      var jsonObject = (JsonObject) message.body();
      var monitorId = jsonObject.getLong(VariableConstants.MONITOR_ID);


      var profiles = CacheStore.getProfilesByMonitor(monitorId);

      for (Long profile : profiles) {
        var profileObject = CacheStore.getProfile(profile);
        var recordedValue = jsonObject.getString(profileObject.getString(VariableConstants.METRIC_VALUE));

        var longValue = BigDecimal.valueOf(Double.parseDouble(recordedValue)).longValue();

        System.out.println("recorded value " + recordedValue);
        var monitorAlertMap = CacheStore.getAlertForMonitorId(monitorId);
        processAlert(monitorAlertMap, profile, monitorId, longValue, profileObject);

      }

    });

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




}
