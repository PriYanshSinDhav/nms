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

  private PgPool client ;


  @Override
  public void start(Promise<Void> startPromise)  {
    try{

      client = DatabaseConfig.getDatabaseClient();
      vertx.eventBus().localConsumer(EventBusConstants.CHECK_AND_ADD_ALERT, message -> {


        var jsonObject = (JsonObject) message.body();
        var monitorId = Long.valueOf(jsonObject.getString(VariableConstants.MONITOR_ID));



        var profiles = CacheStore.getProfilesByMonitor(monitorId);

      for (Long profile : profiles) {
        try {
          var profileObject = CacheStore.getProfile(profile);
          var recordedValue = jsonObject.getString(profileObject.getString(VariableConstants.METRIC_VALUE));
          var longValue = BigDecimal.valueOf(Double.parseDouble(recordedValue)).longValue();
          System.out.println("recorded value " + recordedValue);
          var monitorAlertMap = CacheStore.getAlertForMonitorId(monitorId);
          processAlert(monitorAlertMap, profile, monitorId, longValue, profileObject);

        } catch (Exception e) {
          System.out.println(e);
        }



      }

      }).exceptionHandler(System.out::println);
      startPromise.complete();
    } catch (Exception e) {
      startPromise.fail(e);
    }
  }

  private void processAlert(Map<Long, JsonObject> monitorAlertMap, Long profileId, Long monitorId, Long value, JsonObject profileObject) {
    try{
      var alertJson = createAlertJson(monitorId, profileId, value);

      if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_3)) {
        addOrUpdateAlert( profileId, alertJson, VariableConstants.CRITICAL,monitorId);
        System.out.println("Critical alert generated");
      } else if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_2)) {
        addOrUpdateAlert(profileId, alertJson, VariableConstants.SEVERE,monitorId);
      } else if (value >= profileObject.getLong(VariableConstants.ALERT_LEVEL_1)) {
        addOrUpdateAlert( profileId, alertJson, VariableConstants.WARNING,monitorId);
      } else {
        clearAlertIfExists(monitorAlertMap, profileId,monitorId);
      }
    } catch (Exception e) {
      System.out.println(e);
    }

  }

  private void addOrUpdateAlert(Long profileId, JsonObject alertJson, String alertLevel,Long monitorId) {
    try {
      alertJson.put(VariableConstants.ALERT_LEVEL, alertLevel);
      CacheStore.updateAlert(monitorId,profileId,alertJson);
      insertAlert(alertJson.getLong(VariableConstants.MONITOR_ID),profileId,alertJson.getLong(VariableConstants.VALUE),alertLevel);

    } catch (Exception e) {
      System.out.println(e);
    }
  }

  private void clearAlertIfExists(Map<Long, JsonObject> monitorAlertMap, Long profileId,Long monitorId) {
    monitorAlertMap.computeIfPresent(profileId, (key, alert) -> {
      alert.put(VariableConstants.CLEARED, true);
      return alert;
    });
    CacheStore.clearAlert(monitorId,profileId);
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
