package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.DatabaseConstants;
import com.motadata.utility.JsonObjectUtility;
import com.motadata.utility.ResponseConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;

public class DeviceType  {

  private PgPool client;


  public DeviceType() {
  }



  public void init(Router router) {
    this.client = DatabaseConfig.getDatabaseClient();
    router.post("/create").handler(this::createDeviceType);
    router.get("/get").handler(this::getDeviceType);
    router.post("/get").handler(this::getDeviceTypeWithPagination);
    router.get("/metric/get/:id").handler(this::getMetricsForDeviceType);


  }

  private void getMetricsForDeviceType(RoutingContext routingContext) {

    var deviceTypeId = routingContext.pathParam("id");

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

  private void getDeviceTypeWithPagination(RoutingContext routingContext) {

    JsonObject requestBody = routingContext.body().asJsonObject();

    Long pageNumber = requestBody.getLong("pageNumber");
    Long pageSize = requestBody.getLong("pageSize");

    String sql = "SELECT * FROM NMS_DEVICETYPE LIMIT $1 OFFSET $2";
    client.preparedQuery(sql).execute(Tuple.of(pageSize,pageSize * pageNumber)).onSuccess(rows -> {

      List<JsonObject> deviceTypes = new ArrayList<>();

      rows.forEach(row -> deviceTypes.add(new JsonObject()
        .put("deviceTypeId",row.getValue("devicetypeud"))
        .put("name",row.getValue("name"))));

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,deviceTypes));
    }).onFailure(err-> {
      routingContext.fail(500, err);
    });
  }

  private void getDeviceType(RoutingContext routingContext) {

    String sql = "SELECT * FROM NMS_DEVICETYPE";
    client.preparedQuery(sql).execute().onSuccess(res -> {
      List<JsonObject> deviceTypes = new ArrayList<>();

      for (Row r : res) {
        deviceTypes.add(new JsonObject().put("id",r.getValue("id"))
          .put("name",r.getValue("name"))
        );
      }
      JsonObject jsonObject = new JsonObject();
      jsonObject.put("response",deviceTypes);

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,jsonObject));

    }).onFailure(e -> routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG)));

  }


  private void createDeviceType(RoutingContext routingContext){


    JsonObject requestBody = routingContext.body().asJsonObject();
    String sql = "INSERT INTO NMS_DEVICETYPE (id , name) values ($1 , $2) returning id ";

    client.preparedQuery(sql).execute(Tuple.of(requestBody.getLong("id"),requestBody.getString("name")));

  }

}
