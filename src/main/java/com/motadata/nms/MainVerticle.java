package com.motadata.nms;

import com.motadata.api.*;
import com.motadata.polling.PollingVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

public class MainVerticle extends AbstractVerticle {



  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new MainVerticle());

  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {


    Router router = Router.router(vertx);

    router.route().handler(BodyHandler.create());
    vertx.deployVerticle(new CredentialProfile(router))
      .compose(id -> vertx.deployVerticle(new Device(router)))
      .compose(id-> vertx.deployVerticle(new Monitor(router)))
      .compose(id-> vertx.deployVerticle(new Profile(router)))
      .compose(id-> vertx.deployVerticle(new Alert(router)))
      .compose(id -> vertx.deployVerticle(new Metric(router)))
      .compose(id-> vertx.deployVerticle(new MonitorProfileRel(router)))
      .compose(id -> vertx.deployVerticle(new PollingVerticle()))
      .compose(id -> vertx.createHttpServer()
        .requestHandler(router)
        .listen(8080))
      .onSuccess(server -> {
        startPromise.complete();
        System.out.println("HTTP server started on port 8080");
      })
      .onFailure(startPromise::fail);

  }
}
