package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;

import static com.motadata.constants.QueryConstants.GET_ALL_ALERTS_BY_MONITOR;
import static com.motadata.constants.QueryConstants.GET_ALL_ALERTS_BY_PROFILE;

public class Alert  {

  private PgPool client ;


  public void init(Router router) {
    this.client = DatabaseConfig.getDatabaseClient();
    router.post("/monitor").handler(this::getAlertDetailsByMonitorId);
    router.post("/get").handler(this::getAllAlerts);

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

    client.preparedQuery(GET_ALL_ALERTS_BY_PROFILE).execute(Tuple.of(profileId,level,pageSize,pageNumber*pageSize,profileIdIsNUll,levelIsNUll))

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


    client.preparedQuery(GET_ALL_ALERTS_BY_MONITOR).execute(Tuple.of(monitorId,level,pageSize,pageNumber*pageSize))
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
