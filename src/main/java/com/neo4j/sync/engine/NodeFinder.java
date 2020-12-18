package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.Label;

import java.util.Map;

public class NodeFinder {
    private final JSONObject event;


    public NodeFinder(JSONObject event) {

        this.event = event;

    }

    public String[] getPrimaryKey() throws JSONException {
        Map<String, Object> pk = TransactionDataParser.getPrimaryKey(event);

        String[] primaryKey = new String[2];

        for (Map.Entry<String, Object> entry : pk.entrySet()) {
            primaryKey[0] = entry.getValue().toString();
            primaryKey[1] = entry.getValue().toString();


        }
        return primaryKey;
    }

    public String[] getPrimaryKey(NodeDirection direction) throws JSONException {
        Map<String, Object> pk = TransactionDataParser.getPrimaryKey(event, direction);

        String[] primaryKey = new String[2];

        for (Map.Entry<String, Object> entry : pk.entrySet()) {
            primaryKey[0] = entry.getKey();
            primaryKey[1] = entry.getValue().toString();


        }
        return primaryKey;
    }

    public Label getSearchLabel() throws JSONException {
        String[] labels = TransactionDataParser.getNodeLabels(event);

        return Label.label(labels[0]);
    }

    public Label getSearchLabel(NodeDirection direction) throws JSONException {
        String[] labels = TransactionDataParser.getNodeLabels(event, direction);

        return Label.label(labels[0]);
    }


}
