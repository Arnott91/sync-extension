package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.List;
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
    public List<String> getPrimaryKey() throws JSONException {

        // in this case the primary key should always be the uuid of the node
        // regardless of what the client believes to be the primary key
        // therefore there will be only one primary key value.
        List<String> primaryKey = new ArrayList<>();
        for (Map.Entry<String, Object> entry : TransactionDataParser.getPrimaryKey(event).entrySet()) {
            primaryKey.add(entry.getKey());
            primaryKey.add(entry.getValue().toString());
        }
        return primaryKey;
    }

    public List<String> getPrimaryKey(NodeDirection direction) throws JSONException {
        // in this case the primary key should always be the uuid of the node
        // regardless of what the client believes to be the primary key
        // therefore there will be only one primary key value.
        List<String> primaryKey = new ArrayList<>();
        for (Map.Entry<String, Object> entry : TransactionDataParser.getPrimaryKey(event, direction).entrySet()) {
            primaryKey.add(entry.getKey());
            primaryKey.add(entry.getValue().toString());
        }
        return primaryKey;
    }

    public Label getSearchLabel() throws JSONException {
        /*
        Iterate over transaction labels, returning the first label that is not "LocalTx".
        This should be the primary key.
         */
        for (String label : TransactionDataParser.getNodeLabels(event)) {
            if (!label.equals(TransactionDataParser.LOCAL_TRANSACTION_LABEL)) {
                return Label.label(label);
            }
        }
        return null;
    }

    public Label getSearchLabel(NodeDirection direction) throws JSONException {
        for (String label : TransactionDataParser.getNodeLabels(event, direction)) {
            if (!label.equals(TransactionDataParser.LOCAL_TRANSACTION_LABEL)) {
                return Label.label(label);
            }
        }
        return null;
    }


}
