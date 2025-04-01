package com.motadata.utility;

import io.vertx.core.json.JsonObject;

public class JsonObjectUtility {


  public static <T> JsonObject getResponseJsonObject(Long responseCode , String message , T jsonObject){

    JsonObject responseJsonObject = new JsonObject();
    responseJsonObject.put("code" , responseCode);
    responseJsonObject.put("message",message);
    responseJsonObject.put("response",jsonObject);

    return responseJsonObject;
  }
  public static JsonObject getResponseJsonObject(Long responseCode , String message ){

    JsonObject responseJsonObject = new JsonObject();
    responseJsonObject.put("code" , responseCode);
    responseJsonObject.put("message",message);


    return responseJsonObject;
  }
}
