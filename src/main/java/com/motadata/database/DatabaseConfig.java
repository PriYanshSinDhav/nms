package com.motadata.database;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

public class DatabaseConfig {

  private static PgPool client;

  public  static PgPool getDatabaseClient(Vertx vertx){

    if (client == null) {
      PgConnectOptions connectOptions = new PgConnectOptions()
        .setHost("localhost")
        .setPort(5432)
        .setDatabase("ncm")
        .setUser("root")
        .setPassword("root");

      PoolOptions poolOptions = new PoolOptions().setMaxSize(5);
      client = PgPool.pool(vertx, connectOptions, poolOptions);
    }
    return client;
  }
}
