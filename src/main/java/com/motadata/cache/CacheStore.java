package com.motadata.cache;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.DatabaseConstants;
import com.motadata.utility.VariableConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.motadata.constants.QueryConstants.*;

public class CacheStore extends AbstractVerticle {

  PgPool client;

  private static final Map<Long, JsonObject> CREDENTIAL_MAP = new ConcurrentHashMap<>();

  private static final Map<Long, JsonObject> MONITOR_MAP = new ConcurrentHashMap<>();

  private static final Map<Long, JsonObject> PROFILE_MAP = new ConcurrentHashMap<>();

  private static final Map<Long, JsonObject> METRIC_MAP = new ConcurrentHashMap<>();

  private static final Map<Long, List<Long>> MONITOR_PROFILE_REL_MAP = new ConcurrentHashMap<>();

  private static final Map<Long, Map<Long, JsonObject>> ALERT_MAP = new ConcurrentHashMap<>();


  @Override
  public void start()  {
    initializesMap();
  }


  private void initializesMap() {

    client = DatabaseConfig.getDatabaseClient(vertx);

    initializeCredentialMap();

    initializeMonitorMap();

    initializeProfileMap();

    initializeMetricMap();

    initializeMonitorProfileRelMap();
  }

  private void initializeMonitorProfileRelMap() {
    client.preparedQuery(GET_MONITOR_PROFILE_REL_SQL).execute().onSuccess(res -> {

      res.forEach(row -> {

        MONITOR_PROFILE_REL_MAP.computeIfAbsent(row.getLong(DatabaseConstants.MONITOR_ID), value -> new ArrayList<>()).add(row.getLong(DatabaseConstants.PROFILE_ID));

      });

    });

  }

  private void initializeMetricMap() {
    client.preparedQuery(GET_METRIC_SQL).execute().onSuccess(rows -> {
      rows.forEach(row -> METRIC_MAP
        .put(row.getLong(DatabaseConstants.METRIC_ID),
          new JsonObject()
            .put(VariableConstants.NAME, row.getString(DatabaseConstants.NAME))
            .put(VariableConstants.DEVICE_TYPE_ID, row.getLong(DatabaseConstants.DEVICE_TYPE_ID))
            .put(VariableConstants.ALERTABLE, row.getBoolean(DatabaseConstants.ALERTABLE))
            .put(VariableConstants.METRIC_VALUE, row.getString(DatabaseConstants.METRIC_VALUE))
        ));

    }).onFailure(System.out::println);
  }

  private void initializeProfileMap() {
    client.preparedQuery(GET_PROFILES_SQL).execute().onSuccess(rows -> {

      rows.forEach(row -> PROFILE_MAP.put(row.getLong(DatabaseConstants.PROFILE_ID), new JsonObject()
        .put(VariableConstants.NAME, row.getString(DatabaseConstants.NAME))
        .put(VariableConstants.METRIC_ID, row.getLong(DatabaseConstants.METRIC_ID))
        .put(VariableConstants.METRIC_VALUE, row.getString(DatabaseConstants.METRIC_VALUE))
        .put(VariableConstants.ALERT_LEVEL_1, row.getLong(DatabaseConstants.ALERT_LEVEL_1))
        .put(VariableConstants.ALERT_LEVEL_2, row.getLong(DatabaseConstants.ALERT_LEVEL_2))
        .put(VariableConstants.ALERT_LEVEL_3, row.getLong(DatabaseConstants.ALERT_LEVEL_3))
      ));
    }).onFailure(err -> {
      System.out.println(err);

    });
  }


  private void initializeMonitorMap() {

    client.preparedQuery(QUERY_GET_ALL_MONITORS).execute().onSuccess(res -> res.forEach(this::accept));

  }

  private void initializeCredentialMap() {

    client.preparedQuery(QUERY_SELECT_ALL_CREDENTIALS).execute()
      .onSuccess(rows -> {
        rows.forEach(row -> CREDENTIAL_MAP.put(row.getLong(DatabaseConstants.ID),
          new JsonObject().put(VariableConstants.USERNAME, row.getValue(VariableConstants.USERNAME))
            .put(VariableConstants.PASSWORD, row.getValue(VariableConstants.PASSWORD)
            )));

      });
  }


  public static void addCredentialProfile(Long id, JsonObject credentialProfile) {

    CREDENTIAL_MAP.computeIfAbsent(id, value -> credentialProfile);

  }

  public static void updateCredentialProfile(Long id, JsonObject credentialProfile) {

    CREDENTIAL_MAP.replace(id, credentialProfile);

  }

  public static JsonObject getCredentialProfile(Long id) {

    return new JsonObject().mergeIn(CREDENTIAL_MAP.get(id));

  }

  private void accept(Row r) {
    MONITOR_MAP.put(r.getLong(DatabaseConstants.MONITOR_ID),
      new JsonObject()
        .put(VariableConstants.CREDENTIAL_ID, r.getValue(DatabaseConstants.CREDENTIAL_ID))
        .put(VariableConstants.IP_ADDRESS, r.getValue(DatabaseConstants.IP_ADDRESS))
        .put(VariableConstants.POLLING_INTERVAL, r.getValue(DatabaseConstants.POLLING_INTERVAL))
        .put(VariableConstants.REMAINING_INTERVAL, r.getValue(DatabaseConstants.POLLING_INTERVAL))
    );
  }


  public static void addMonitor(Long id, JsonObject monitor) {

    MONITOR_MAP.putIfAbsent(id, monitor);

  }

  public static void updateMonitor(Long id, JsonObject monitor) {

    MONITOR_MAP.replace(id, monitor);

  }

  public static JsonObject getMonitor(Long id) {

    return new JsonObject().mergeIn(MONITOR_MAP.get(id));

  }

  public static ConcurrentHashMap<Long, JsonObject> getAllMonitors() {


    return new ConcurrentHashMap<>(MONITOR_MAP);

  }


  public static void addProfile(Long id, JsonObject profile) {

    PROFILE_MAP.putIfAbsent(id, profile);

  }

  public static void updateProfile(Long id, JsonObject profile) {

    PROFILE_MAP.replace(id, profile);

  }

  public static JsonObject getProfile(Long id) {

    return new JsonObject().mergeIn(PROFILE_MAP.get(id));

  }

  public static void addMetric(Long metricId, JsonObject jsonObject) {
    METRIC_MAP.put(metricId, jsonObject);
  }


  public static JsonObject getMetric(Long id) {

    return new JsonObject().mergeIn(METRIC_MAP.get(id));

  }

  public static void addProfilesToCacheMap(Long monitorId, List<Long> profileIds) {

    MONITOR_PROFILE_REL_MAP.computeIfPresent(monitorId, (key, value) -> {
      value.addAll(profileIds);
      return value;
    });
  }

  public static void addProfilesToCacheMap(List<Long> monitors, Long profile) {

    monitors.forEach(monitor -> {
      MONITOR_PROFILE_REL_MAP.computeIfAbsent(monitor, value -> new ArrayList<>()).add(profile);
    });

  }

  public static List<Long> getProfilesByMonitor(Long monitorId) {
    return new ArrayList<>(MONITOR_PROFILE_REL_MAP.getOrDefault(monitorId, Collections.emptyList()));
  }

  public static boolean shouldPoll(Long monitorId) {

    return MONITOR_MAP.getOrDefault(monitorId, new JsonObject())

      .getLong(VariableConstants.REMAINING_INTERVAL, 0L)

      .equals(0L);

  }

  public static void resetRemainingInterval(Long monitorId) {
    MONITOR_MAP.computeIfPresent(monitorId, (key, value) -> {
      value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.POLLING_INTERVAL, 0L));
      return value;
    });
  }

  public static void decrementRemainingInterval(Long monitorId) {
    MONITOR_MAP.computeIfPresent(monitorId, (key, value) -> {
      value.put(VariableConstants.REMAINING_INTERVAL, value.getLong(VariableConstants.REMAINING_INTERVAL, 0L) - 10L);
//      System.out.println(value);
      return value;
    });
  }

  public static Map<Long, JsonObject> getAlertForMonitorId(Long monitorId) {
    var monitorAlertMap = ALERT_MAP.computeIfAbsent(monitorId, key -> new ConcurrentHashMap<>());
    return new ConcurrentHashMap<>(monitorAlertMap);

  }

  public static void updateAlert(Long monitorId, Map<Long, JsonObject> monitorAlertMap) {
    ALERT_MAP.replace(monitorId, monitorAlertMap);
  }
}
