package com.motadata.api;

import com.motadata.database.DatabaseConfig;
import com.motadata.utility.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CredentialProfile extends AbstractVerticle {

  private static final String GET_CREDENTIAL_PAGE_SQL = "SELECT * FROM NMS_CREDENTIALS LIMIT $1 OFFSET $2";

  PgPool client ;

  private final Router router;

  private static final Map<Long,JsonObject> CREDENTIALMAP = new ConcurrentHashMap<>();

  private static final String RESPONSE_CREDENTIAL_NOT_FOUND = "Credential Profile not found";

  private static final String RESPONSE_CREDENTIAL_DOES_NOT_EXIST = "Credential Profile does not exist";

  private static final String QUERY_SELECT_ALL_CREDENTIALS = "SELECT * FROM NMS_CREDENTIALS";

  private static final String QUERY_SELECT_CREDENTIAL_BY_ID = "SELECT * FROM NMS_CREDENTIALS WHERE id = $1";

  private static final String QUERY_INSERT_CREDENTIAL = "INSERT INTO NMS_CREDENTIALS(USERNAME,PASSWORD) VALUES ($1,$2) RETURNING id";

  private static final String QUERY_UPDATE_CREDENTIAL = "UPDATE NMS_CREDENTIALS SET USERNAME = $2 , PASSWORD = $3 WHERE id = $1 RETURNING id";

  public CredentialProfile(Router router) {
    this.router = router;
  }

  @Override
  public void start() throws Exception {
    this.client =  DatabaseConfig.getDatabaseClient(vertx);


    createRouters();

    consumerForCredentialProfile();

    addCredentialProfilesToMap();

    createConsumerForCredentialId();


  }

  private void createRouters() {
    router.post("/credential").handler(this::createCredentialProfile);
    router.post("/get/credential").handler(this::getAllCredentialProfiles);
    router.get("/credential").handler(this::getAllCredentialProfilesWithoutPagination);
    router.post("/credential/update/:id").handler(this::updateCredentialProfile);
    router.get("/credential/:id").handler(this::getCredentialProfileById);
  }

  private void createConsumerForCredentialId() {

    vertx.eventBus().localConsumer(EventBusConstants.EVENT_GET_CREDENTIAL,message -> {
      var credentialId = (long) message.body();
      message.reply(CREDENTIALMAP.get(credentialId));
    });
  }

  private void addCredentialProfilesToMap() {

    client.preparedQuery(QUERY_SELECT_ALL_CREDENTIALS).execute()
      .onSuccess(rows -> {
        rows.forEach(row -> CREDENTIALMAP.put(row.getLong(DatabaseConstants.ID),
          new JsonObject().put(VariableConstants.USERNAME, row.getValue(VariableConstants.USERNAME))
          .put(VariableConstants.PASSWORD, row.getValue(VariableConstants.PASSWORD)
          )));

      });
  }


  private void consumerForCredentialProfile() {

    vertx.eventBus().localConsumer(EventBusConstants.EVENT_GET_CREDENTIAL_PROFILE, message -> {
      Long credentialId = (Long) message.body();

      client.preparedQuery(QUERY_SELECT_CREDENTIAL_BY_ID)
        .execute(Tuple.of(credentialId))
        .onSuccess(result -> {
          if (result.size() > 0) {
            Row row = result.iterator().next();
            JsonObject credentialProfile = new JsonObject()
              .put(VariableConstants.ID,row.getValue(VariableConstants.ID))
              .put(VariableConstants.USERNAME,row.getValue(VariableConstants.USERNAME))
              .put(VariableConstants.PASSWORD,row.getValue(VariableConstants.PASSWORD));

            message.reply(credentialProfile); // Send response to DiscoverDeviceVerticle
          } else {
            message.fail(404, "Credential not found");
          }
        })
        .onFailure(err -> message.fail(500, err.getMessage()));
    });


  }

  private void getCredentialProfileById(RoutingContext routingContext) {

    Long credentialProfileId = Long.parseLong(routingContext.pathParam(VariableConstants.ID));


    client.preparedQuery(QUERY_SELECT_CREDENTIAL_BY_ID).execute(Tuple.of(credentialProfileId))
      .onSuccess(res -> {

        if (!res.iterator().hasNext()) {
          routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,RESPONSE_CREDENTIAL_DOES_NOT_EXIST));
          return;
        }
        Row row = res.iterator().next();
        JsonObject jsonObject = new JsonObject();
        jsonObject.put(VariableConstants.USERNAME,row.getValue(VariableConstants.USERNAME));
        jsonObject.put(VariableConstants.ID,row.getValue(VariableConstants.ID));
        jsonObject.put(VariableConstants.PASSWORD,row.getValue(VariableConstants.PASSWORD));
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,jsonObject));
      }).onFailure(err -> {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG));

      });
  }



  private void updateCredentialProfile(RoutingContext routingContext) {

    Long id = Long.parseLong(routingContext.pathParam(VariableConstants.ID));

    JsonObject requestBody =  routingContext.body().asJsonObject();

    var username = requestBody.getString(VariableConstants.USERNAME);
    var password = requestBody.getString(VariableConstants.PASSWORD);

    if(username == null || password == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Invalid Data : username and password required to update credential profile"));
    }

    client.preparedQuery(QUERY_UPDATE_CREDENTIAL).execute(Tuple.of(id,requestBody.getString(username),requestBody.getString(password)))
      .onSuccess(res -> {

        if (!res.iterator().hasNext()) {
          routingContext.json(JsonObjectUtility.getResponseJsonObject( ResponseConstants.ERROR,RESPONSE_CREDENTIAL_NOT_FOUND));
          return;
        }
        routingContext.json(JsonObjectUtility.
          getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,new JsonObject().put("id",res.iterator().next().getValue("id"))));
      }).onFailure( err ->
        routingContext.json( JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,ResponseConstants.ERROR_MSG))
      );

  }

  private void createCredentialProfile(RoutingContext routingContext) {

    JsonObject credentialProfileRequest =  routingContext.body().asJsonObject();

    var username = credentialProfileRequest.getString(VariableConstants.USERNAME);
    var password = credentialProfileRequest.getString(VariableConstants.PASSWORD);

    if(username == null || password == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Invalid Data : username and password required to generate credential profile"));
    }

    client.preparedQuery(QUERY_INSERT_CREDENTIAL)
      .execute(Tuple.of(username,password))
      .onSuccess( res -> {
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,
          res.iterator().next().getInteger(DatabaseConstants.ID)));
        CREDENTIALMAP.put(Long.valueOf(res.iterator().next().getInteger(DatabaseConstants.ID)),
          new JsonObject().put(VariableConstants.USERNAME, username)
            .put(VariableConstants.PASSWORD,password));
      })
      .onFailure(err -> routingContext.fail(500 , err));

  }


  private void getAllCredentialProfiles(RoutingContext routingContext){

    JsonObject pageObject = routingContext.body().asJsonObject();

    if(pageObject == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Pagination Data required for getting credential profiles")) ;
      return;
    }
    var pageSize = pageObject.getLong(VariableConstants.PAGE_SIZE);
    var pageNumber = pageObject.getLong(VariableConstants.PAGE_NUMBER);

    if(pageSize == null || pageNumber == null){
      routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.ERROR,"Pagination Data required for getting credential profiles")) ;
      return;
    }

    client.preparedQuery(GET_CREDENTIAL_PAGE_SQL ).execute(Tuple.of(pageSize,pageSize*pageNumber))
      .onSuccess(res ->{
        List<JsonObject> credentialProfiles = new ArrayList<>();
        res.forEach(e -> credentialProfiles.add(new JsonObject()
          .put(VariableConstants.ID,e.getValue(DatabaseConstants.ID))
          .put(VariableConstants.USERNAME,e.getValue(DatabaseConstants.USERNAME))
          .put(VariableConstants.PASSWORD,e.getValue(DatabaseConstants.PASSWORD))
        ));
        routingContext.json(credentialProfiles);

      }).onFailure(err -> {
        System.out.println(" error occured " + err.getMessage());
        routingContext.fail(500,err);
      });
  }

  private void getAllCredentialProfilesWithoutPagination(RoutingContext routingContext) {


    client.preparedQuery(QUERY_SELECT_ALL_CREDENTIALS).execute()
      .onSuccess(res ->{
        List<JsonObject> credentialProfiles = new ArrayList<>();
        res.forEach(e -> credentialProfiles.add(new JsonObject()
          .put(VariableConstants.ID,e.getValue(DatabaseConstants.ID))
          .put(VariableConstants.USERNAME,e.getValue(DatabaseConstants.USERNAME))
          .put(VariableConstants.PASSWORD,e.getValue(DatabaseConstants.PASSWORD))
        ));
        routingContext.json(JsonObjectUtility.getResponseJsonObject(ResponseConstants.SUCCESS,ResponseConstants.SUCCESS_MSG,credentialProfiles));

      }).onFailure(err -> {
        System.out.println(" error occured " + err.getMessage());
        routingContext.fail(500,err);
      });

  }


}
