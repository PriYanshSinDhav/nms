package com.motadata.nms;

import com.motadata.api.*;
import com.motadata.cache.CacheStore;
import com.motadata.database.DatabaseConfig;
import com.motadata.polling.PollingVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle {


  private static Vertx vertx = Vertx.vertx();

  public static void main(String[] args) {


    Router router = Router.router(getVertx());

    router.route().handler(BodyHandler.create());




    List<String> deployedVerticles = new ArrayList<>();

    vertx.deployVerticle(new APIServer(router))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.deployVerticle(new CacheStore()))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.deployVerticle(new MonitorVerticle()))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.deployVerticle(new AlertVerticle()))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.deployVerticle(new MonitoringVerticle()))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.deployVerticle(PollingVerticle.class.getName(),
        new DeploymentOptions().setInstances(2)))
      .onSuccess(deployedVerticles::add)
      .compose(id -> vertx.createHttpServer()
        .requestHandler(router)
        .listen(8080))
      .onSuccess(server -> {
        System.out.println("HTTP server started on port 8080");
      })
      .onFailure(err -> {
        System.err.println("Startup failed: " + err.getMessage());

        var client = DatabaseConfig.getDatabaseClient();
        if (client != null) {
          client.close().onComplete(ar -> {
            if (ar.succeeded()) {
              System.out.println("Database client closed successfully .");

            } else {
              System.out.println("Error closing database client: " + ar.cause().getMessage());

            }
          });
        }

        System.out.println(deployedVerticles);
        List<Future> undeployFutures = deployedVerticles.stream()
          .map(vertx::undeploy)
          .collect(Collectors.toList());

        CompositeFuture.all(undeployFutures)
          .onComplete(ar -> {
            System.out.println("All deployed verticles undeployed. Exiting...");
            System.exit(1);
          });
      });

  }

  public static Vertx getVertx(){
    return vertx;
  }
}
