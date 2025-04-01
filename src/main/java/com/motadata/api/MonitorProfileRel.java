package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MonitorProfileRel extends AbstractVerticle {

  private static final String ADD_MONITOR_PROFILE_REL_SQL = "INSERT INTO nms_monitor_profile (monitorid , profileid) values ($1,$2) ";
  private static final String GET_MONITOR_PROFILE_REL_SQL = "select * from nms_monitor_profile " ;
    private static final String GET_ALL_PROFILES_BY_MONITOR_SQL = "select p.name as profilename from nms_monitor_profile mp join nms_profile p on mp.profileid = p.profileid where mp.monitorid = $1";
  private static final String GET_ALL_MONITORS_BY_PROFILE_SQL = "select p.ipaddress as ipaddress  from nms_monitor_profile mp join nms_monitor m on mp.monitorid = m.monitorid where mp.profileid = $1";
  private final Router router;

  private static Map<Long,List<Long>> monitorProfileRel = new ConcurrentHashMap<>();

  PgPool client;

  public MonitorProfileRel(Router router) {
    this.router = router;
  }

  public static Map<Long,List<Long>>  getMonitorProfileRelMap(){

    return monitorProfileRel;
  }


  @Override
  public void start() throws Exception {

    client = DatabaseConfig.getDatabaseClient(vertx);

    router.get("/monitor/:id").handler(this::getMonitorsByProfileId);
    router.get("/profile/:id").handler(this::getProfilesByMonitorId);
    router.post().handler(this::addProfileToMonitor);
    router.post().handler(this::addMonitorToProfile);

    addProfilesToCache();

    vertx.eventBus().consumer(EventBusConstants.GET_PROFILES_FOR_MONITOR,message -> {
      var monitorId = (Long)message.body();
      message.reply(monitorProfileRel.get(monitorId));
    });



  }

  private void addMonitorToProfile(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();

    var profileId = requestBody.getLong(VariableConstants.PROFILE_ID);

    List<Long> monitors = requestBody.getJsonArray("monitorIds").getList();

    if (profileId == null || monitors.isEmpty()) {
      routingContext.fail(400, new Throwable("Invalid request: monitorId or profileIds missing"));
      return;
    }

    List<Tuple> batch  = new ArrayList<>();

    for (Long monitorId : monitors) {
      batch.add(Tuple.of(monitorId,profileId));
    }


    client.preparedQuery(ADD_MONITOR_PROFILE_REL_SQL).executeBatch(batch).onSuccess(res-> {

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG));
      addProfilesToCache( monitors,profileId);
    }).onFailure(err-> {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error occured while adding monitors to profile reason -> " + err.getMessage()));
    });


  }

  private void addProfileToMonitor(RoutingContext routingContext) {

    var requestBody = routingContext.body().asJsonObject();

    var monitorId = requestBody.getLong(DatabaseConstants.MONITOR_ID);

    List<Long> profiles = requestBody.getJsonArray("profileIds").getList();

    if (monitorId == null || profiles.isEmpty()) {
      routingContext.fail(400, new Throwable("Invalid request: monitorId or profileIds missing"));
      return;
    }

    List<Tuple> batch  = new ArrayList<>();

    for (Long profile : profiles) {
      batch.add(Tuple.of(monitorId,profile));
    }


    client.preparedQuery(ADD_MONITOR_PROFILE_REL_SQL).executeBatch(batch).onSuccess(res-> {

      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG));

      addProfilesToCache(monitorId,profiles);

    }).onFailure(err-> {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Error occured while adding profile to monitor reason -> " + err.getMessage()));
    });
  }

  private void addProfilesToCache() {
    client.preparedQuery(GET_MONITOR_PROFILE_REL_SQL).execute().onSuccess(res->{

      res.forEach(row -> {

        monitorProfileRel.computeIfAbsent(row.getLong(DatabaseConstants.MONITOR_ID),value -> new ArrayList<>()).add(row.getLong(DatabaseConstants.PROFILE_ID));

      });

    });


  }

  private void addProfilesToCache(Long monitorId, List<Long> profileIds){

    monitorProfileRel.computeIfPresent(monitorId,(key,value) -> {
      value.addAll(profileIds);
      return value;
    });
  }

  private void addProfilesToCache( List<Long> monitors,Long profile){

    monitors.forEach(monitor-> {
      monitorProfileRel.computeIfAbsent(monitor,value-> new ArrayList<>()).add(profile);
    });

  }

  private void getProfilesByMonitorId(RoutingContext routingContext) {

    var monitorId = routingContext.pathParam(VariableConstants.ID);

    client.preparedQuery(GET_ALL_PROFILES_BY_MONITOR_SQL)
      .execute(Tuple.of(monitorId))
      .onSuccess(rows -> {
        var profiles = new ArrayList<JsonObject>();
        rows.forEach(row -> profiles.add(new JsonObject().put(VariableConstants.PROFILE_NAME,row.getValue(DatabaseConstants.PROFILE_NAME))));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,profiles));
      })
      .onFailure(err->
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG + "reason " + err.getMessage() ))
      );
  }

  private void getMonitorsByProfileId(RoutingContext routingContext) {

    var profileId = routingContext.pathParam(VariableConstants.ID);


    client.preparedQuery(GET_ALL_MONITORS_BY_PROFILE_SQL)
      .execute(Tuple.of(profileId))
      .onSuccess(rows -> {
        var profiles = new ArrayList<JsonObject>();
        rows.forEach(row -> profiles.add(new JsonObject().put(VariableConstants.IP_ADDRESS,row.getValue(DatabaseConstants.IP_ADDRESS))));

        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,profiles));
      })
      .onFailure(err->
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG + "reason " + err.getMessage() ))
      );

  }
}
