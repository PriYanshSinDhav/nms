package com.motadata.api;

import com.motadata.cache.CacheStore;
import com.motadata.utility.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;

import static com.motadata.constants.QueryConstants.*;

public class CredentialProfile {

  private static final String GET_CREDENTIAL_PAGE_SQL = "SELECT * FROM NMS_CREDENTIALS LIMIT $1 OFFSET $2";

  PgPool client;


  public static final String RESPONSE_CREDENTIAL_NOT_FOUND = "Credential Profile not found";

  public static final String RESPONSE_CREDENTIAL_DOES_NOT_EXIST = "Credential Profile does not exist";


  public void init(Router router, PgPool client) {
    this.client = client;
    router.post("/create").handler(this::createCredentialProfile);
    router.post("/get").handler(this::getAllCredentialProfiles);
    router.get("/all").handler(this::getAllCredentialProfilesWithoutPagination);
    router.post("/update/:id").handler(this::updateCredentialProfile);
    router.get("/:id").handler(this::getCredentialProfileById);

  }


  private void getCredentialProfileById(RoutingContext routingContext) {

    Long credentialProfileId = Long.parseLong(routingContext.pathParam(VariableConstants.ID));


    client.preparedQuery(QUERY_SELECT_CREDENTIAL_BY_ID).execute(Tuple.of(credentialProfileId))
      .onSuccess(res -> {

        if (!res.iterator().hasNext()) {
          routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, RESPONSE_CREDENTIAL_DOES_NOT_EXIST));
          return;
        }
        Row row = res.iterator().next();
        JsonObject jsonObject = new JsonObject();
        jsonObject.put(VariableConstants.USERNAME, row.getValue(VariableConstants.USERNAME));
        jsonObject.put(VariableConstants.ID, row.getValue(VariableConstants.ID));
        jsonObject.put(VariableConstants.PASSWORD, row.getValue(VariableConstants.PASSWORD));
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, jsonObject));
      }).onFailure(err -> {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, ResponseConstants.ERROR_MSG));

      });
  }


  private void updateCredentialProfile(RoutingContext routingContext) {

    Long id = Long.parseLong(routingContext.pathParam(VariableConstants.ID));

    JsonObject requestBody = routingContext.body().asJsonObject();

    var username = requestBody.getString(VariableConstants.USERNAME);
    var password = requestBody.getString(VariableConstants.PASSWORD);

    if (username == null || password == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Invalid Data : username and password required to update credential profile"));
    }

    client.preparedQuery(QUERY_UPDATE_CREDENTIAL).execute(Tuple.of(id, username, password))
      .onSuccess(res -> {

        if (!res.iterator().hasNext()) {
          routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, RESPONSE_CREDENTIAL_NOT_FOUND));
          return;
        }
        routingContext.json(JsonObjectUtility.
          getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, new JsonObject().put(VariableConstants.ID, res.iterator().next().getValue(DatabaseConstants.ID))));

        CacheStore.updateCredentialProfile(res.iterator().next().getLong(DatabaseConstants.ID), new JsonObject().put(VariableConstants.USERNAME, username).put(VariableConstants.PASSWORD, password));
      }).onFailure(err ->
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, ResponseConstants.ERROR_MSG))
      );

  }

  private void createCredentialProfile(RoutingContext routingContext) {

    JsonObject credentialProfileRequest = routingContext.body().asJsonObject();

    var username = credentialProfileRequest.getString(VariableConstants.USERNAME);
    var password = credentialProfileRequest.getString(VariableConstants.PASSWORD);

    if (username == null || password == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Invalid Data : username and password required to generate credential profile"));
    }

    client.preparedQuery(QUERY_INSERT_CREDENTIAL)
      .execute(Tuple.of(username, password))
      .onSuccess(res -> {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG,
          res.iterator().next().getInteger(DatabaseConstants.ID)));

        CacheStore.addCredentialProfile(res.iterator().next().getLong(DatabaseConstants.ID), new JsonObject().put(VariableConstants.USERNAME, username)
          .put(VariableConstants.PASSWORD, password));

      })
      .onFailure(err -> routingContext.fail(500, err));

  }


  private void getAllCredentialProfiles(RoutingContext routingContext) {

    JsonObject pageObject = routingContext.body().asJsonObject();

    if (pageObject == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Pagination Data required for getting credential profiles"));
      return;
    }
    var pageSize = pageObject.getLong(VariableConstants.PAGE_SIZE);
    var pageNumber = pageObject.getLong(VariableConstants.PAGE_NUMBER);

    if (pageSize == null || pageNumber == null) {
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR, "Pagination Data required for getting credential profiles"));
      return;
    }

    client.preparedQuery(GET_CREDENTIAL_PAGE_SQL).execute(Tuple.of(pageSize, pageSize * pageNumber))
      .onSuccess(res -> {
        List<JsonObject> credentialProfiles = new ArrayList<>();
        res.forEach(e -> credentialProfiles.add(new JsonObject()
          .put(VariableConstants.ID, e.getValue(DatabaseConstants.ID))
          .put(VariableConstants.USERNAME, e.getValue(DatabaseConstants.USERNAME))
          .put(VariableConstants.PASSWORD, e.getValue(DatabaseConstants.PASSWORD))
        ));
        routingContext.json(credentialProfiles);

      }).onFailure(err -> {
        System.out.println(" error occured " + err.getMessage());
        routingContext.fail(500, err);
      });
  }

  private void getAllCredentialProfilesWithoutPagination(RoutingContext routingContext) {


    client.preparedQuery(QUERY_SELECT_ALL_CREDENTIALS).execute()
      .onSuccess(res -> {
        List<JsonObject> credentialProfiles = new ArrayList<>();
        res.forEach(e -> credentialProfiles.add(new JsonObject()
          .put(VariableConstants.ID, e.getValue(DatabaseConstants.ID))
          .put(VariableConstants.USERNAME, e.getValue(DatabaseConstants.USERNAME))
          .put(VariableConstants.PASSWORD, e.getValue(DatabaseConstants.PASSWORD))
        ));
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS, ResponseConstants.SUCCESS_MSG, credentialProfiles));

      }).onFailure(err -> {
        System.out.println(" error occured " + err.getMessage());
        routingContext.fail(500, err);
      });

  }


}
