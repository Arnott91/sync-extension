package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionDataParser {




    public static JSONObject TranslateTransactionData(String transactionData) throws JSONException {

        // wrap the JSON data in a JSON object to make it easy to work with

        return new JSONObject(transactionData);

    };
    
    public static List<Map<String, JSONObject>> getTransactionEvents(JSONObject entireTransaction) throws JSONException {

        // the JSON object we get from the entire transaction is actually an array of distinct events
        // each defined by change type. So below what we do is grab the events array object
        // and break it up into an array of json objects, each representing a unique event.
        

        JSONArray events = entireTransaction.getJSONArray("transactionEvents");
        List<Map<String, JSONObject>> eventsList = new ArrayList<>();
        Map<String,JSONObject> changeTypeEventMap = new HashMap<>();

        // now that we have our list, we need to segregate into event change types.
        // so here we add each event to a list of maps: (change type string, event jason object)



        for (int i = 0; i > events.length(); i++)  {
            JSONObject event = (JSONObject) events.get(i);
           changeTypeEventMap.put(event.get("changeType").toString(), event);
           eventsList.add(changeTypeEventMap);
        };


        return eventsList;
    }


    private static Map<String,String> getKeyValueComponents(JSONObject event, ParseType parseType) throws JSONException {

        // providing a generic "get me key - value pairs" seems like a useful thing.


        switch (parseType) {
            case NODE_PROPERTIES: return getNodeProperties(event);
            case PRIMARY_KEY: return getPrimaryKey(event);
            case REL_PROPERTIES: return getRelationProperties(event);
            default: return null;

        }
    }

    public static Map<String, String> getNodeProperties(JSONObject nodeEvent) throws JSONException {

        return ((JSONObject) nodeEvent.get("allProperties")).toMap();

    }
    public static Map<String, String> getPrimaryKey(JSONObject nodeEvent) throws JSONException {


            return ((JSONObject) nodeEvent.get("primaryKey")).toMap();


    }

    public static Map<String, String> getPrimaryKey(JSONObject nodeEvent, NodeDirection direction) throws JSONException {

        switch (direction) {
            case START: return  ((JSONObject) nodeEvent.get("primaryKey")).toMap();
            case TARGET: return ((JSONObject) nodeEvent.get("targetPrimaryKey")).toMap();
            default: return null;
        }

    }




    public static String[] getNodeLabels(JSONObject nodeEvent) throws JSONException {

        return  nodeEvent.get("labels").toString().split(",");

    }

    public static String[] getNodeLabels(JSONObject nodeEvent, NodeDirection direction) throws JSONException {

        switch (direction) {
            case START: return  nodeEvent.get("labels").toString().split(",");
            case TARGET: return nodeEvent.get("targetLabels").toString().split(",");
            default: return null;
        }

    }


    public static String getRelationType(JSONObject relationEvent) throws JSONException {

        // unlike node labels, relationships can have only one type.

        return relationEvent.get("relationshipLabel").toString();
    }

    public static Map<String,String> getRelationProperties(JSONObject relationEvent) throws JSONException {

        return  ((JSONObject) relationEvent.get("allProperties")).toMap();
    }














}
