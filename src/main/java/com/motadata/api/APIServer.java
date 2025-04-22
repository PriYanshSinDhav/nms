package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.DatabaseConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.pgclient.PgPool;

import static com.motadata.constants.QueryConstants.*;

public class APIServer extends AbstractVerticle {


  private final Router router;

  PgPool client;

  public APIServer(Router router) {
    this.router = router;
  }

  @Override
  public void start()  {

    this.client = DatabaseConfig.getDatabaseClient(vertx);

    addRouters();
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

    new CredentialProfile().init(credentialRouter, client);
    new DeviceType().init(deviceTypeRouter, client);
    new Profile().init(profileRouter, client);
    new Alert().init(alertRouter, client);
    new Metric().init(metricRouter, client);
    new Device().init(deviceRouter, client);

  }


}
