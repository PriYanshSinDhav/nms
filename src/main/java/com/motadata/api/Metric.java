package com.motadata.api;

import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.motadata.constants.QueryConstants.*;

public class Metric  {



  PgPool client;



  public void init(Router router,PgPool client) {
    this.client = client;
    router.post("/devicetype/get/:id").handler(this::getMetricsForDeviceType);
    router.post("/add").handler(this::addMetric);

  }


  private void addMetric(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();

    if(requestBody == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Data required to create Metric"));
      return;
    }

    var name = requestBody.getString(VariableConstants.NAME);
    var metricValue = requestBody.getString(VariableConstants.METRIC_VALUE);
    var alertable = requestBody.getBoolean(VariableConstants.ALERTABLE);
    var deviceTypeId = requestBody.getLong(VariableConstants.DEVICE_TYPE_ID);

    if (name == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Metric name is  required to create Metric"));
      return;
    } else if (metricValue == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Metric value is  required to create Metric"));
      return;
    }else if (alertable == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Alertable is required to create Metric to decide whether metric is alertable or not "));
      return;
    }else if (deviceTypeId == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Device Type id is  required to create Metric"));
      return;
    }

    client.preparedQuery(ADD_METRIC_SQL).execute(Tuple.of(name,deviceTypeId,alertable,metricValue)).onSuccess(rows -> {
      var metricId = rows.iterator().next().getLong(DatabaseConstants.METRIC_ID);

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,metricId));

      // add to cache
      CacheStore.addMetric(metricId, new JsonObject()
        .put(VariableConstants.NAME,name)
        .put(VariableConstants.METRIC_VALUE,metricValue)
        .put(VariableConstants.ALERTABLE,alertable)
        .put(VariableConstants.DEVICE_TYPE_ID,deviceTypeId)
        );

    }).onFailure(err -> JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, err.getMessage()));
  }


  private void getMetricsForDeviceType(RoutingContext routingContext) {

    var deviceTypeId = Long.valueOf(routingContext.pathParam(VariableConstants.ID));

    client.preparedQuery("select * from NMS_METRIC WHERE devicetypeid = $1").execute(Tuple.of(deviceTypeId)).onSuccess(rows -> {
      var metrics = new ArrayList<JsonObject>();
      rows.forEach(row -> {


        metrics.add(new JsonObject().put(VariableConstants.METRIC_ID,row.getLong(DatabaseConstants.METRIC_ID))
          .put(VariableConstants.ALERTABLE,row.getBoolean(DatabaseConstants.ALERTABLE))
          .put(VariableConstants.METRIC_VALUE,row.getString(DatabaseConstants.METRIC_VALUE))
          .put(VariableConstants.NAME,row.getString( DatabaseConstants.NAME)));

      });
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,metrics));

    }).onFailure(err -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,err.getMessage())));
  }



}
