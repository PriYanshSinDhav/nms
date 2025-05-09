package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import static com.motadata.constants.QueryConstants.ADD_MONITORING_DATA_SQL;


public class MonitoringVerticle extends AbstractVerticle {

  private PgPool client ;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    try{
      client = DatabaseConfig.getDatabaseClient();
      vertx.eventBus().localConsumer(EventBusConstants.ADD_METRIC_DETAILS,message -> {

        var json = (JsonObject)message.body();
        var monitorId = Long.valueOf(json.getString(VariableConstants.MONITOR_ID));
        json.remove(VariableConstants.MONITOR_ID);

        client.preparedQuery(ADD_MONITORING_DATA_SQL)
          .execute(Tuple.of(monitorId, json))
          .onSuccess(rows -> {
            System.out.println("monitoring done for monitorid " + monitorId);
          })
          .onFailure(err -> System.out.println(err));


      });
      startPromise.complete();
    } catch (Exception e) {
      startPromise.fail(e);
    }
  }
}
