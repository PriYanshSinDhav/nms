package com.motadata.api;

import com.motadata.cache.CacheStore;
import com.motadata.utility.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.motadata.constants.QueryConstants.*;

public class Profile {


  PgPool client;



  public void init(Router router, PgPool client) {
    this.client = client;
    router.post("/create").handler(this::createProfile);
    router.get("/get").handler(this::getAllProfiles);
    router.post("/get").handler(this::getProfilesWithPagination);


  }

  private void getProfilesWithPagination(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();

    var pageNumber = requestBody.getLong(VariableConstants.PAGE_NUMBER);
    var pageSize = requestBody.getLong(VariableConstants.PAGE_SIZE);


    client.preparedQuery(GET_PROFILE_PAGINATION_SQL).execute(Tuple.of(pageSize, pageNumber * pageSize)).onSuccess(rows -> {
      var profiles = new ArrayList<JsonObject>();

      rows.forEach(row -> profiles.add(new JsonObject()
        .put(VariableConstants.PROFILE_ID, row.getLong(DatabaseConstants.PROFILE_ID))
        .put(VariableConstants.NAME, row.getString(DatabaseConstants.NAME))
        .put(VariableConstants.METRIC_VALUE, row.getString(DatabaseConstants.METRIC_VALUE))
        .put(VariableConstants.ALERT_LEVEL_1, row.getLong(DatabaseConstants.ALERT_LEVEL_1))
        .put(VariableConstants.ALERT_LEVEL_2, row.getLong(DatabaseConstants.ALERT_LEVEL_2))
        .put(VariableConstants.ALERT_LEVEL_3, row.getLong(DatabaseConstants.ALERT_LEVEL_3))
      ));

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, profiles));

    }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, ResponseConstants.ERROR_MSG)));


  }


  private void getAllProfiles(RoutingContext routingContext) {

    client.preparedQuery(GET_PROFILES_SQL).execute().onSuccess(rows -> {
      var profiles = new ArrayList<JsonObject>();

      rows.forEach(row -> profiles.add(new JsonObject()
        .put(VariableConstants.PROFILE_ID, row.getLong(DatabaseConstants.PROFILE_ID))
        .put(VariableConstants.NAME, row.getString(DatabaseConstants.NAME))
        .put(VariableConstants.METRIC_VALUE, row.getString(DatabaseConstants.METRIC_VALUE))
        .put(VariableConstants.ALERT_LEVEL_1, row.getLong(DatabaseConstants.ALERT_LEVEL_1))
        .put(VariableConstants.ALERT_LEVEL_2, row.getLong(DatabaseConstants.ALERT_LEVEL_2))
        .put(VariableConstants.ALERT_LEVEL_3, row.getLong(DatabaseConstants.ALERT_LEVEL_3))
      ));

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, profiles));

    }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, err.getMessage())));

  }

  private void createProfile(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();

    if (requestBody == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Data required to create profile"));
      return;
    }


    var name = requestBody.getString(VariableConstants.NAME);
    var alertLevel1 = requestBody.getLong(VariableConstants.ALERT_LEVEL_1);
    var alertLevel2 = requestBody.getLong(VariableConstants.ALERT_LEVEL_2);
    var alertLevel3 = requestBody.getLong(VariableConstants.ALERT_LEVEL_3);
    var metricId = requestBody.getLong(VariableConstants.METRIC_ID);

    if (name == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Profile name required to create profile"));
      return;
    } else if (alertLevel1 == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Alert level 1 required to create profile"));
      return;
    } else if (alertLevel2 == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Alert level 2 required to create profile"));
      return;
    } else if (alertLevel3 == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Alert level 3 required to create profile"));
      return;
    } else if (metricId == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Metric Id required to create profile"));
      return;
    }

    var metricObject = CacheStore.getMetric(metricId);

    if (metricObject == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Given metric id not found"));
      return;
    }
    var metricValue = metricObject.getString(VariableConstants.METRIC_VALUE);
    client.preparedQuery(CREATE_PROFILE_SQL).execute(Tuple.of(metricId, name, alertLevel1, alertLevel2, alertLevel3, metricValue))
      .onSuccess(res -> {

        var profileId = res.iterator().next().getLong(DatabaseConstants.PROFILE_ID);
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, profileId));
        CacheStore.addProfile(profileId, new JsonObject().put(VariableConstants.NAME, name).put(VariableConstants.ALERT_LEVEL_1, alertLevel1));
      }).onFailure(err -> {
        System.out.println(err);
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Error occurred while trying to add profile reason :- " + err.getMessage()));
      });


  }


}
