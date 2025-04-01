package com.motadata.api;

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

public class Metric extends AbstractVerticle {

  private static final String ADD_METRIC_SQL = "INSERT INTO NMS_METRIC (name , devicetypeid ,alertable , metricvalue) values ($1,$2,$3,$4) returning metricid";
  private static final String GET_METRIC_SQL = "SELECT * FROM NMS_METRIC";

  private final Router router;

  PgPool client;

  private static final Map<Long,JsonObject> METRIC_CACHE_MAP = new ConcurrentHashMap<>();


  public Metric(Router router) {
    this.router = router;
  }

  @Override
  public void start() throws Exception {

    client = DatabaseConfig.getDatabaseClient(vertx);
    router.get("/metric/:id").handler(this :: getMetricByDeviceType);
    router.get("/devicetype/metric/get/:id").handler(this::getMetricsForDeviceType);
    router.post("/metric/add").handler(this::addMetric);
//    router.post("").handler(this::getMetricWithPagination);

    initializeMetricCacheMap();

    vertx.eventBus().consumer(EventBusConstants.EVENT_GET_METRIC,handler -> {
      var metricId = (Long)handler.body();

      var metricObject = METRIC_CACHE_MAP.get(metricId);

      handler.reply(metricObject);
    });
  }

  private void initializeMetricCacheMap() {

    client.preparedQuery(GET_METRIC_SQL).execute().onSuccess(rows -> {
      rows.forEach(row -> METRIC_CACHE_MAP
        .put(row.getLong(DatabaseConstants.METRIC_ID),
          new JsonObject()
            .put(VariableConstants.NAME,row.getString(DatabaseConstants.NAME))
            .put(VariableConstants.DEVICE_TYPE_ID, row.getLong(DatabaseConstants.DEVICE_TYPE_ID))
            .put(VariableConstants.ALERTABLE,row.getBoolean(DatabaseConstants.ALERTABLE) )
            .put(VariableConstants.METRIC_VALUE,row.getString(DatabaseConstants.METRIC_VALUE))
        ));

    }).onFailure(System.out::println);
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
      addMetricToCacheMap(metricId, new JsonObject()
        .put(VariableConstants.NAME,name)
        .put(VariableConstants.METRIC_VALUE,metricValue)
        .put(VariableConstants.ALERTABLE,alertable)
        .put(VariableConstants.DEVICE_TYPE_ID,deviceTypeId)
        );

    }).onFailure(err -> JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, err.getMessage()));
  }

  private void addMetricToCacheMap(Long metricId, JsonObject jsonObject) {
    METRIC_CACHE_MAP.put(metricId,jsonObject);
  }


  private void getMetricByDeviceType(RoutingContext routingContext) {

    Long typeId = Long.valueOf(routingContext.pathParam(VariableConstants.ID));
    String sql = "SELECT * FROM  NMS_METRIC M WHERE M.typeid = $1";

    client.preparedQuery(sql).execute(Tuple.of(typeId))
      .onSuccess(res -> {
        List<JsonObject> metrics = new ArrayList<>();

        for (Row r : res) {
          metrics.add(new JsonObject().put(VariableConstants.ID,r.getValue(DatabaseConstants.ID))
            .put(VariableConstants.NAME,r.getValue(DatabaseConstants.NAME))
            .put(VariableConstants.ALERTABLE,r.getValue(DatabaseConstants.ALERTABLE))
            .put(VariableConstants.METRIC_VALUE,r.getValue(DatabaseConstants.METRIC_VALUE)));
        }
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,metrics));

      })
      .onFailure(err -> JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,err.getMessage()));
  }

  private void getMetricsForDeviceType(RoutingContext routingContext) {

    var deviceTypeId = routingContext.pathParam(VariableConstants.ID);

    client.preparedQuery("select * from NMS_METRIC WHERE devicetypeid = $1").execute(Tuple.of(deviceTypeId)).onSuccess(rows -> {

      rows.forEach(row -> {
        var metrics = new ArrayList<JsonObject>();

        metrics.add(new JsonObject().put(VariableConstants.METRIC_ID, DatabaseConstants.METRIC_ID)
          .put(VariableConstants.ALERTABLE,DatabaseConstants.ALERTABLE)
          .put(VariableConstants.METRIC_VALUE,DatabaseConstants.METRIC_VALUE)
          .put(VariableConstants.NAME,DatabaseConstants.NAME));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,metrics));
      });

    }).onFailure(err -> JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,err.getMessage()));
  }



}
