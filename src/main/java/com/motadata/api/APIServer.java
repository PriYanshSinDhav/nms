package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.pgclient.PgPool;


public class APIServer extends AbstractVerticle {


  private final Router router;


  public APIServer(Router router) {
    this.router = router;
  }

  @Override
  public void start(Promise<Void> startPromise)  {

    try {
      addRouters();
      System.out.println("api verticle");
      startPromise.complete();
    } catch (Exception e) {
      startPromise.fail(e);
    }
  }

  private void addRouters() {
    var credentialRouter = Router.router(vertx);
    var deviceTypeRouter = Router.router(vertx);
    var profileRouter = Router.router(vertx);
    var alertRouter = Router.router(vertx);
    var metricRouter = Router.router(vertx);
    var monitorProfileRelRouter = Router.router(vertx);
    var deviceRouter = Router.router(vertx);


    router.route("/credential/*").subRouter(credentialRouter);
    router.route("/devicetype/*").subRouter(deviceTypeRouter);
    router.route("/profile/*").subRouter(profileRouter);
    router.route("/alert/*").subRouter(alertRouter);
    router.route("/metric/*").subRouter(metricRouter);
    router.route("/monitorprofile/*").subRouter(monitorProfileRelRouter);
    router.route("/device/*").subRouter(deviceRouter);

    new CredentialProfile().init(credentialRouter);
    new DeviceType().init(deviceTypeRouter);
    new Profile().init(profileRouter);
    new Alert().init(alertRouter);
    new Metric().init(metricRouter);
    new Device().init(deviceRouter);

  }


}
