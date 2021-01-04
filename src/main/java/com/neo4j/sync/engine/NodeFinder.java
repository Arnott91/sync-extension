package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.Label;
import java.util.Map;

/**
 * com.neo4j.sync.engine.NodeFinder provides primary keys and labels used for finding nodes
 * in the com.neo4j.sync.engine.GraphWriter and TransactionDataHandler classes.
 * Attempting to be modular.
 *
 * @author Chris Upkes
 */

public class NodeFinder {
    private final JSONObject event;

    public NodeFinder(JSONObject event) {
        this.event = event;
    }
    // I chose to use arrays because all results are finite...maybe I'm old-school.
    public String[] getPrimaryKey() throws JSONException {

        // in this case the primary key should always be the uuid of the node
        // regardless of what the client believes to be the primary key
        // therefore there will be only one primary key value.
        Map<String, Object> pk = TransactionDataParser.getPrimaryKey(event);
        String[] primaryKey = new String[2];
        for (Map.Entry<String, Object> entry : pk.entrySet()) {
            primaryKey[0] = entry.getKey();
            primaryKey[1] = entry.getValue().toString();
        }

        return primaryKey;
    }

    public String[] getPrimaryKey(NodeDirection direction) throws JSONException {

        // in this case the primary key should always be the uuid of the node
        // regardless of what the client believes to be the primary key
        // therefore there will be only one primary key value.
        Map<String, Object> pk = TransactionDataParser.getPrimaryKey(event, direction);
        String[] primaryKey = new String[2];
        for (Map.Entry<String, Object> entry : pk.entrySet()) {
            primaryKey[0] = entry.getKey();
            primaryKey[1] = entry.getValue().toString();
        }
        return primaryKey;
    }

    public Label getSearchLabel() throws JSONException {

        // grab the labels from the transaction
        String[] labels = TransactionDataParser.getNodeLabels(event);
        // only need to grab the first label - that should be the primary
        return Label.label(labels[0]);
    }

    public Label getSearchLabel(NodeDirection direction) throws JSONException {

        // grab the labels from the transaction
        String[] labels = TransactionDataParser.getNodeLabels(event, direction);
        // only need to grab the first label-  that should be the primary
        return Label.label(labels[0]);
    }


}
