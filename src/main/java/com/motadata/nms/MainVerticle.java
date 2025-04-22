package com.motadata.nms;

import com.motadata.api.*;
import com.motadata.cache.CacheStore;
import com.motadata.polling.PollingVerticle;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle {



  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    Router router = Router.router(vertx);

    router.route().handler(BodyHandler.create());

    vertx.deployVerticle(new APIServer(router))
      .compose(id -> vertx.deployVerticle(new CacheStore()))
      .compose(id -> vertx.deployVerticle(new PollingVerticle()))
      .compose(id -> vertx.createHttpServer()
        .requestHandler(router)
        .listen(8080))
      .onSuccess(server -> {
        System.out.println("HTTP server started on port 8080");
      })
      .onFailure(err-> System.out.println(err));


  }
}
