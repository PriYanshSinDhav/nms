package com.motadata.api;

import com.motadata.cache.CacheStore;
import com.motadata.utility.EventBusConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;

import java.util.Map;

public class MonitorVerticle extends AbstractVerticle {


  @Override
  public void start()  {

    vertx.setPeriodic(10L * 1000L,handler-> {
      for (Map.Entry<Long, JsonObject> longJsonObjectEntry : CacheStore.getAllMonitors().entrySet()) {
        vertx.eventBus().send(EventBusConstants.POLL_MONITOR,new JsonObject().put(VariableConstants.MONITOR_ID,longJsonObjectEntry.getKey()).put(VariableConstants.VALUE,longJsonObjectEntry.getValue()) );

      }
    });



  }
}
