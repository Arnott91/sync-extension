package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import scala.util.parsing.json.JSON;

import java.util.Map;

public class NodeFinder {
    private final JSONObject event;


    public NodeFinder(JSONObject event){

        this.event = event;

    }

    public String[] getPrimaryKey() throws JSONException {
        Map<String, String> pk  = TransactionDataParser.getPrimaryKey(event);

        String[] primaryKey = new String[2];

        for (Map.Entry<String, String> entry : pk.entrySet()) {
            primaryKey[0]= entry.getValue();
            primaryKey[1] = entry.getValue();


        }
        return primaryKey;
    }

    public String[] getPrimaryKey(NodeDirection direction) throws JSONException {
        Map<String, String> pk  = TransactionDataParser.getPrimaryKey(event, direction);

        String[] primaryKey = new String[2];

        for (Map.Entry<String, String> entry : pk.entrySet()) {
            primaryKey[0]= entry.getValue();
            primaryKey[1] = entry.getValue();


        }
        return primaryKey;
    }

    public Label getSearchLabel () throws JSONException {
        String[] labels = TransactionDataParser.getNodeLabels(event);

        return Label.label(labels[0]);
    }

    public Label getSearchLabel (NodeDirection direction) throws JSONException {
        String[] labels = TransactionDataParser.getNodeLabels(event, direction);

        return Label.label(labels[0]);
    }




}
