package com.neo4j.sync.engine;

import org.apache.commons.lang3.ArrayUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionDataParser {

    public static final String NODE_PROPERTY_CHANGE = "NodePropertyChange";
    public static final String TARGET_PRIMARY_KEY = "targetPrimaryKey";
    private static final String LOCAL_TRANSACTION_LABEL = "com.neo4j.sync.engine.LocalTx";
    private static final String ADD_NODE_LABEL_KEY = "nodeLabels";
    private static final String TARGET_NODE_LABEL_KEY = "targetNodeLabels";
    public static final String ADD_NODE_PROPERTIES = "AddNodeProperties";
    public static final String ADD_RELATION = "AddRelation";
    public static final String ADD_NODE = "AddNode";
    public static final String DELETE_RELATION = "DeleteRelation";
    public static final String DELETE_NODE = "DeleteNode";
    public static final String RELATION_PROPERTY_CHANGE = "RelationPropertyChange";
    public static final String PRIMARY_KEY = "primaryKey";
    public static final String PRIMARY_KEY1 = PRIMARY_KEY;
    public static final String PROPERTIES_KEY = "propertyName";
    public static final String NEW_VALUE = "newValue";
    public static final String ALL_PROPERTIES_KEY = "allProperties";
    public static final String CHANGE_TYPE_KEY = "changeType";
    public static final String RELATIONSHIP_LABEL_KEY = "relationshipLabel";
    public static final String TRANSACTION_EVENTS_KEY = "transactionEvents";
    public static final String REGEX = ",";


    public static JSONObject TranslateTransactionData(String transactionData) throws JSONException {

        // wrap the JSON data in a JSON object to make it easy to work with

        return new JSONObject(transactionData);

    }
    
    public static List<Map<String, JSONObject>> getTransactionEvents(JSONObject entireTransaction) throws JSONException {

        // the JSON object we get from the entire transaction is actually an array of distinct events
        // each defined by change type. So below what we do is grab the events array object
        // and break it up into an array of json objects, each representing a unique event.
        

        JSONArray events = entireTransaction.getJSONArray(TRANSACTION_EVENTS_KEY);
        List<Map<String, JSONObject>> eventsList = new ArrayList<>();


        // now that we have our list, we need to segregate into event change types.
        // so here we add each event to a list of maps: (change type string, event jason object)


        // delete nodes sorted first
        sortEvents(events, eventsList, DELETE_NODE);
        // delete relationships second
        sortEvents(events, eventsList, DELETE_RELATION);
        // add nodes third
        sortEvents(events, eventsList, ADD_NODE);
        // add relation fourth
        sortEvents(events, eventsList, ADD_RELATION);
        // add node properties fifth
        sortEvents(events, eventsList,  ADD_NODE_PROPERTIES);
        // add node property change sixth
        sortEvents(events, eventsList, NODE_PROPERTY_CHANGE);
        // rel property change seventh
        sortEvents(events, eventsList, RELATION_PROPERTY_CHANGE);


        return eventsList;
    }

    private static void sortEvents(JSONArray events, List<Map<String, JSONObject>> eventsList, String changeType) throws JSONException {
        for (int i = 0; i < events.length(); i++)  {
            JSONObject event = (JSONObject) events.get(i);
            if (event.get(CHANGE_TYPE_KEY).toString().equals(changeType)) {
                Map<String,JSONObject> changeTypeEventMap = new HashMap<>();
                changeTypeEventMap.put(event.get(CHANGE_TYPE_KEY).toString(), event);
                eventsList.add(changeTypeEventMap);
            }
        }
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

        return ((JSONObject) nodeEvent.get(ALL_PROPERTIES_KEY)).toMap();

    }
    public static Map<String, String> getPrimaryKey(JSONObject nodeEvent) throws JSONException {


            return ((JSONObject) nodeEvent.get(PRIMARY_KEY)).toMap();


    }

    public static Map<String, String> getPrimaryKey(JSONObject nodeEvent, NodeDirection direction) throws JSONException {

        switch (direction) {
            case START: return  ((JSONObject) nodeEvent.get(PRIMARY_KEY1)).toMap();
            case TARGET: return ((JSONObject) nodeEvent.get(TARGET_PRIMARY_KEY)).toMap();
            default: return null;

        }

    }




    public static String[] getNodeLabels(JSONObject nodeEvent) throws JSONException {

        // add LOCAL_TX label
        String [] transactionLabels =  nodeEvent.get(ADD_NODE_LABEL_KEY).toString().split(REGEX);
        String [] txLabelsPlusLocal = new String[transactionLabels.length + 1];
        System.arraycopy(transactionLabels, 0, txLabelsPlusLocal, 0, transactionLabels.length);

        txLabelsPlusLocal[transactionLabels.length] = LOCAL_TRANSACTION_LABEL;

        return txLabelsPlusLocal;

    }

    public static String[] getTargetNodeLabels(JSONObject nodeEvent) throws JSONException {

        // add LOCAL_TX label
        String [] transactionLabels =  nodeEvent.get(TARGET_NODE_LABEL_KEY).toString().split(REGEX);
        ArrayUtils.add(transactionLabels, LOCAL_TRANSACTION_LABEL);
        return transactionLabels;

    }


    public static String[] getNodeLabels(JSONObject nodeEvent, NodeDirection direction) throws JSONException {

        switch (direction) {
            case START: return  nodeEvent.get(ADD_NODE_LABEL_KEY).toString().split(REGEX);
            case TARGET: return nodeEvent.get(TARGET_NODE_LABEL_KEY).toString().split(REGEX);
            default: return null;
        }

    }

    public static Map<String, String> getChangedProperties(JSONObject event) throws JSONException{



        JSONArray beforeAndAfterArray = event.getJSONArray("" +
                "");

        Map<String, String> changedProperties = new HashMap<>();


        for (int i = 0; i < beforeAndAfterArray.length(); i++){

            JSONObject banda = (JSONObject) beforeAndAfterArray.get(i);
            if (!banda.get(NEW_VALUE).equals(null)) {
                changedProperties.put(banda.getString(PROPERTIES_KEY), banda.getString(NEW_VALUE));
            }


        }
        return changedProperties;


    }

    public static String[] getRemovedProperties(JSONObject nodeEvent) throws JSONException {

        JSONArray beforeAndAfterArray = nodeEvent.getJSONArray(PROPERTIES_KEY);

        List<String> removed = new ArrayList<>();

        for (int i = 0; i < beforeAndAfterArray.length(); i++){

            JSONObject banda = (JSONObject) beforeAndAfterArray.get(i);
            if (banda.get(NEW_VALUE).equals(null)){
                removed.add(banda.get(PROPERTIES_KEY).toString());

            }


        }
        return removed.toArray(new String[removed.size()]);

    }


    public static String getRelationType(JSONObject relationEvent) throws JSONException {

        // unlike node labels, relationships can have only one type.

        return relationEvent.get(RELATIONSHIP_LABEL_KEY).toString();
    }

    public static Map<String,String> getRelationProperties(JSONObject relationEvent) throws JSONException {

        return  ((JSONObject) relationEvent.get(ALL_PROPERTIES_KEY)).toMap();
    }














}
