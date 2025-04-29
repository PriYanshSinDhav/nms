package com.motadata.nms;

import com.motadata.api.*;
import com.motadata.cache.CacheStore;
import com.motadata.polling.PollingVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle {


  private static Vertx vertx = Vertx.vertx();

  public static void main(String[] args) {


    Router router = Router.router(getVertx());

    router.route().handler(BodyHandler.create());


    vertx.deployVerticle(new APIServer(router))
      .compose(id -> vertx.deployVerticle(new CacheStore()))
      .compose(id -> vertx.deployVerticle(PollingVerticle.class.getName(),new DeploymentOptions().setInstances(2)))
      .compose(id -> vertx.deployVerticle(new MonitorVerticle()))
      .compose(id-> vertx.deployVerticle(new AlertVerticle()))
      .compose(id-> vertx.deployVerticle(new MonitoringVerticle()))
      .compose(id -> vertx.createHttpServer()
        .requestHandler(router)
        .listen(8080))
      .onSuccess(server -> {
        System.out.println("HTTP server started on port 8080");
      })
      .onFailure(err-> System.out.println(err));


  }

  public static Vertx getVertx(){
    return vertx;
  }
}
