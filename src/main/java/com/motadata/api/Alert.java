package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Alert extends AbstractVerticle {

  private static final String ADD_ALERT_SQL = "INSERT INTO NMS_ALERT (monitorid,profileid,level,value) values ($1,$2,$3,$4)";

  private static final Map<Long, Map<Long,JsonObject>> ALERT_CACHE_MAP = new ConcurrentHashMap();

  private final  Router router;

  PgPool client ;

  public Alert(Router router) {
    this.router = router;
  }

  @Override
  public void start() throws Exception {


    client = DatabaseConfig.getDatabaseClient(vertx);

    router.post("/monitor/alerts").handler(this::getAlertDetailsByMonitorId);
    router.post("/alerts").handler(this::getAllAlerts);


    vertx.eventBus().consumer(EventBusConstants.CHECK_AND_ADD_ALERT, message -> {

      var jsonObject = (JsonObject) message.body();
      var profileId = jsonObject.getLong(VariableConstants.PROFILE_ID);
      var monitorId = jsonObject.getLong(VariableConstants.MONITOR_ID);
      var value = jsonObject.getLong(VariableConstants.VALUE);
      var profileObject = jsonObject.getJsonObject(VariableConstants.PROFILE);

      var monitorAlertMap = ALERT_CACHE_MAP.computeIfAbsent(monitorId, key -> new ConcurrentHashMap<>());

      processAlert(monitorAlertMap, profileId, monitorId, value, profileObject);
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


  private void getAllAlerts(RoutingContext routingContext){

    JsonObject requestBody = routingContext.body().asJsonObject();


    Long pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);

    Long pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);

    Long profileId = requestBody.getLong(VariableConstants.PROFILE_ID);

    String level = requestBody.getString("level");

    var profileIdIsNUll = 1L;
    if (profileId != null) {
      profileIdIsNUll = 0L;
    }

    var levelIsNUll = 1L;
    if (level != null && !level.isBlank()) {
      levelIsNUll = 0L;
    }


    String sql = "SELECT n.alertid , n.monitorid , n.profileid , n.level , n.timestamp ,n.value , p.name as profilename, m.ipaddress   FROM NMS_ALERT n join NMS_MONITOR m ON n.monitorid = m.monitorid join NMS_PROFILE p on n.profileid = p.profileid  WHERE  (1 = $5 OR n.profileid = $1)  and (1 = $6 or n.level = $2)  ORDER BY timestamp desc LIMIT $3  OFFSET $4 ";

    client.preparedQuery(sql).execute(Tuple.of(profileId,level,pageSize,pageNumber*pageSize,profileIdIsNUll,levelIsNUll))
      .onSuccess(rows -> {
        var alertDetails = new ArrayList<JsonObject>();

        rows.forEach(row-> {
          alertDetails.add(new JsonObject()
            .put(VariableConstants.ALERT_ID,row.getLong(DatabaseConstants.ALERT_ID))
            .put(VariableConstants.MONITOR_ID,row.getLong(DatabaseConstants.MONITOR_ID))
            .put(VariableConstants.IP_ADDRESS,row.getString(DatabaseConstants.IP_ADDRESS))
            .put(VariableConstants.ALERT_LEVEL,row.getString(DatabaseConstants.ALERT_LEVEL))
            .put(VariableConstants.VALUE,row.getLong(DatabaseConstants.VALUE))
            .put(VariableConstants.GENERATED_AT,row.getValue(DatabaseConstants.TIMESTAMP).toString())
            .put(VariableConstants.PROFILE_NAME,row.getString(DatabaseConstants.PROFILE_NAME))

          );
        });

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,alertDetails));


      })
      .onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error fetching alert details reason :- " + err.getMessage())));

  }

  private void getAlertDetailsByMonitorId(RoutingContext routingContext){

    JsonObject requestBody = routingContext.body().asJsonObject();


    Long pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);

    Long pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);

    Long monitorId = requestBody.getLong(VariableConstants.MONITOR_ID);

    String level = requestBody.getString("level");

    String sql = "SELECT * , n.profileid as profileid , p.name as profilename FROM NMS_ALERT n JOIN NMS_PROFILE p on n.profileid = p.profileid  WHERE  n.monitorid = $1 and n.level = $2 ORDER BY timestamp desc  LIMIT $3  OFFSET $4";

    client.preparedQuery(sql).execute(Tuple.of(monitorId,level,pageSize,pageNumber*pageSize))
      .onSuccess(rows -> {

        var alertDetails = new ArrayList<JsonObject>();

        rows.forEach(row-> {
          alertDetails.add(new JsonObject()
            .put(VariableConstants.ALERT_ID,row.getLong(DatabaseConstants.ALERT_ID))
            .put(VariableConstants.PROFILE_ID,row.getLong(DatabaseConstants.PROFILE_ID))
            .put(VariableConstants.PROFILE_NAME,row.getString(DatabaseConstants.PROFILE_NAME))
            .put(VariableConstants.ALERT_LEVEL,row.getString(DatabaseConstants.ALERT_LEVEL))
            .put(VariableConstants.VALUE,row.getLong(DatabaseConstants.VALUE))
            .put(VariableConstants.GENERATED_AT,row.getValue(DatabaseConstants.TIMESTAMP).toString())

          );
        });

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,alertDetails));


      })
      .onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error fetching alert details reason :- " + err.getMessage())));

  }


}
