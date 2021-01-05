package com.neo4j.sync;

import com.neo4j.sync.engine.TransactionDataParser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TransactionDataParserTest {

    private final String ADD_NODE = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_PROPERTIES = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":null}],\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";

    @Test
    void translateJSONTest() throws JSONException {
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        assertNotNull(graphTxTranslation);
    }

    @Test
    void addNodeTest() throws JSONException {

        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        String changeType = graphTxTranslation.getString("changeType");
        assertEquals("AddNode", changeType);

        JSONObject allProperties = new JSONObject(graphTxTranslation.get("allProperties").toString());

        assertEquals(1, allProperties.length());
        assertEquals("123XYZ", allProperties.get("uuid"));
    }

    @Test
    void addNodeAndPropertyTest() throws JSONException {

        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODES_AND_PROPERTIES);
        String changeType = graphTxTranslation.getString("changeType");
        assertEquals("AddNode", changeType);

        JSONObject allProperties = (JSONObject) graphTxTranslation.get("allProperties");

        assertEquals(2, allProperties.length());
        assertEquals("foo", allProperties.get("testProperty"));
    }

    @Test
    void addMultipleRelationshipsTest() throws JSONException {

        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_MULTIPLE_RELATIONSHIPS);

        JSONObject allProperties = (JSONObject) graphTxTranslation.get("allProperties");

        assertEquals(2, allProperties.length());
        assertEquals("foo", allProperties.get("testProperty"));

    }

    @Test
    void getRemovedNodesTest() throws JSONException {

        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE);

        List<Map<String, JSONObject>> txEvents = TransactionDataParser.getTransactionEvents(graphTxTranslation);

        JSONObject propertyChangeEvent = txEvents.get(0).get("NodePropertyChange");

        JSONArray beforeAndAfterArray = propertyChangeEvent.getJSONArray("properties");

        List<String> removed = new ArrayList<>();

        for (int i = 0; i < beforeAndAfterArray.length(); i++) {

            JSONObject banda = (JSONObject) beforeAndAfterArray.get(i);
            if (banda.get("newValue").equals(null)) {
                removed.add(banda.get("propertyName").toString());

            }
        }
    }
}
