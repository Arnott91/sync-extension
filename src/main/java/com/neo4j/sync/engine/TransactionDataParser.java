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

        return new JSONObject(wrapTransactionMessage(transactionData));

    };
    
    public static List<Map<String, JSONObject>>getTransactionEvents(JSONObject entireTransaction) throws JSONException {
        

        JSONArray events = entireTransaction.getJSONArray("events");
        List<Map<String, JSONObject>> eventsList = new ArrayList<>();
        Map<String,JSONObject> changeTypeEventMap = new HashMap<>();

        // now that we have our list, we need to segregate into event change types.


        for (int i = 0; i > events.length(); i++)  {
            JSONObject event = (JSONObject) events.get(i);
           changeTypeEventMap.put(event.get("changeType").toString(), event);
           eventsList.add(changeTypeEventMap);
        };
        // now that we have our list, we need to segregate into event change types.

        return eventsList;
    }


    private static String wrapTransactionMessage(String transactionData) {
        // remove the surrounding brackets of each transaction message.
        return "{\"events\":" + transactionData + "}";

    };

    public static List<String> getNodeLabels(JSONObject transaction) throws JSONException {

        return (List<String>) transaction.getJSONArray("labels");

    }

    private static Map<String,String> getKeyValueComponents(JSONObject transaction, ParseType parseType) throws JSONException {


        switch (parseType) {
            case PROPERTIES: return getNodeProperties(transaction);
            case PRIMARY_KEY: return getPrimaryKey(transaction);
            default: return null;

        }
    }

    public static Map<String, String> getNodeProperties(JSONObject event) throws JSONException {

        return ((JSONObject) event.get("allProperties")).toMap();

    }

    public static Map<String, String> getPrimaryKey(JSONObject event) throws JSONException {

        return ((JSONObject) event.get("primaryKey")).toMap();

    }

    public static String getRelationTypes(JSONObject event) throws JSONException {

        return event.get("relationshipLabel").toString();
    }

    public static String getRelationProperties(JSONObject event) throws JSONException {

        return event.get("allProperties").toString();
    }














}
