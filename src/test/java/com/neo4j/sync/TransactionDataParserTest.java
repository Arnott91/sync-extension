package com.neo4j.sync;

import com.neo4j.sync.engine.NodeDirection;
import com.neo4j.sync.engine.NodeFinder;
import com.neo4j.sync.engine.TransactionDataParser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import scala.concurrent.java8.FuturesConvertersImpl;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionDataParserTest {

    private final String ADD_NODE = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_PROPERTIES = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":null}],\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    @Inject
    private List<Map<String, JSONObject>> transactionEvents;

    @Test
    void translateJSONTest() throws JSONException {
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        assertNotNull(graphTxTranslation);
    }

    @Test
    void addNodeTest() throws JSONException {

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(ADD_NODE));
        Map<String,JSONObject> event = transactionEvents.get(0);
        for (Map.Entry<String, JSONObject> e : event.entrySet()) {
            String k = e.getKey();
            assertEquals("AddNode", k);
            JSONObject v = e.getValue();
            String[] labels = (String[]) TransactionDataParser.getNodeLabels(v);
            assertEquals("Test", labels[0]);
            // get the collection of properties
            Map<String, Object> properties = TransactionDataParser.getNodeProperties(v);
            assertEquals("123XYZ",properties.get("uuid"));

            }

    }

    @Test
    void addNodeAndPropertyTest() throws JSONException {

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(ADD_NODES_AND_PROPERTIES));
        Map<String,JSONObject> event = transactionEvents.get(0);
        for (Map.Entry<String, JSONObject> e : event.entrySet()) {
            String k = e.getKey();
            assertEquals("AddNode", k);
            JSONObject v = e.getValue();
            String[] labels = (String[]) TransactionDataParser.getNodeLabels(v);
            assertEquals("Test", labels[0]);
            // get the collection of properties
            Map<String, Object> properties = TransactionDataParser.getNodeProperties(v);
            assertEquals("123XYZ",properties.get("uuid"));
            assertEquals("foo",properties.get("testProperty"));

        }

    }

    @Test
    void addMultipleRelationshipsTest() throws JSONException {

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(ADD_MULTIPLE_RELATIONSHIPS));
        Map<String,JSONObject> event = transactionEvents.get(0);
        for (Map.Entry<String, JSONObject> e : event.entrySet()) {
            String k = e.getKey();
            JSONObject v = e.getValue();
            if (k.equals("AddRelation")) {

                NodeFinder finder = new NodeFinder(v);
                Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
                Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

                String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
                String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);
                assertEquals("uuid",startPrimaryKey[0]);
                assertEquals("uuid",targetPrimaryKey[0]);
                assertEquals("Test",startSearchLabel.toString());
                // Map<String, Object> properties = TransactionDataParser.getRelationProperties(v);
            } else {
                String[] labels = (String[]) TransactionDataParser.getNodeLabels(v);
                assertEquals("Test", labels[0]);
                // get the collection of properties
                Map<String, Object> properties = TransactionDataParser.getNodeProperties(v);
                assertEquals("123XYZ",properties.get("uuid"));
            }
        }

    }

    @Test
    void getRemovedNodePropTest() throws JSONException {

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(NODE_PROPERTY_CHANGE));
        Map<String,JSONObject> event = transactionEvents.get(0);
        for (Map.Entry<String, JSONObject> e : event.entrySet()) {
            String k = e.getKey();
            assertEquals("NodePropertyChange", k);
            JSONObject v = e.getValue();
            String[] labels = (String[]) TransactionDataParser.getNodeLabels(v);
            assertEquals("Test", labels[0]);
            // get the collection of properties
            String[] properties = TransactionDataParser.getRemovedProperties(v);
            assertEquals("test", properties[0]);

        }

    }
}
