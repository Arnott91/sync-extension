package com.neo4j.sync;

import com.neo4j.sync.engine.TransactionDataParser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.test.extension.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@TestInstance( TestInstance.Lifecycle.PER_METHOD )

public class TranslatorTest {

    @Inject
    private final String ADD_NODE = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_PROPERTIES = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_RELATIONSHIP = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_PROPERTIES_TO_REL = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{\"weight\":123},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":null}],\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE2 = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":\"bar\"}],\"allProperties\":{\"test\":\"bar\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String REL_PROPERTY_CHANGE1 = "{\"transactionEvents\":[{\"changeType\":\"RelationPropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":[{\"propertyName\":\"weight\",\"oldValue\":1,\"newValue\":2}],\"allProperties\":{\"weight\":2},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";

    @Test
    void TranslateJSONTest() throws JSONException {


            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);

    }

    @Test
    void AddNodeTest() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
            String changeType = graphTxTranslation.getString("changeType");
            Assertions.assertEquals("AddNode", changeType);


            JSONObject allProperties = new JSONObject(graphTxTranslation.get("allProperties").toString());

            Assertions.assertEquals(1,allProperties.length());
            Assertions.assertEquals( "123XYZ",allProperties.get("uuid"));



        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    void AddNodeAndPropertyTest() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODES_AND_PROPERTIES);
            String changeType = graphTxTranslation.getString("changeType");
            Assertions.assertEquals("AddNode", changeType);


            JSONObject allProperties = (JSONObject) graphTxTranslation.get("allProperties");

            Assertions.assertEquals(2,allProperties.length());
            Assertions.assertEquals( "foo",allProperties.get("testProperty"));






        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    void AddMultipleRelationshipsTest() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_MULTIPLE_RELATIONSHIPS);

            //String changeType = graphTxTranslation..getString("changeType");
            //Assertions.assertEquals("AddNode", changeType);


            JSONObject allProperties = (JSONObject) graphTxTranslation.get("allProperties");

            Assertions.assertEquals(2,allProperties.length());
            Assertions.assertEquals( "foo",allProperties.get("testProperty"));






        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    void GetRemovedNodesTest() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE);

            List<Map<String, JSONObject>> txEvents = TransactionDataParser.getTransactionEvents(graphTxTranslation);

            JSONObject propertyChangeEvent = txEvents.get(0).get("NodePropertyChange");

            JSONArray beforeAndAfterArray = propertyChangeEvent.getJSONArray("properties");

            List<String> removed = new ArrayList<>();

            for (int i = 0; i < beforeAndAfterArray.length(); i++){

                JSONObject banda = (JSONObject) beforeAndAfterArray.get(i);
                if (banda.get("newValue").equals(null)){
                    removed.add(banda.get("propertyName").toString());

                }


            }

            System.out.println(removed.get(0));
//            for (Map<String, JSONObject> txEvent : txEvents) {
//
//                System.out.println(txEvent.get("NodePropertyChange").toString());
//
//            }



//            String[] deletedPropertyKeys = TransactionDataParser.getRemovedProperties(txEvents)
//;
//
//            Assertions.assertEquals(2,allProperties.length());
//            Assertions.assertEquals( "foo",allProperties.get("testProperty"));






        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    void GetRemovedNodesTest2() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE);

            List<Map<String, JSONObject>> txEvents = TransactionDataParser.getTransactionEvents(graphTxTranslation);
            JSONObject propertyChangeEvent = txEvents.get(0).get("NodePropertyChange");

            String[] removed = TransactionDataParser.getRemovedProperties(propertyChangeEvent);

            System.out.println(removed[0]);
//            for (Map<String, JSONObject> txEvent : txEvents) {
//
//                System.out.println(txEvent.get("NodePropertyChange").toString());
//
//            }



//            String[] deletedPropertyKeys = TransactionDataParser.getRemovedProperties(txEvents)
//;
//
//            Assertions.assertEquals(2,allProperties.length());
//            Assertions.assertEquals( "foo",allProperties.get("testProperty"));






        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    void GetChangedPropertiesTest() {

        try {
            JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE2);

            List<Map<String, JSONObject>> txEvents = TransactionDataParser.getTransactionEvents(graphTxTranslation);
            JSONObject propertyChangeEvent = txEvents.get(0).get("NodePropertyChange");

           Map<String, String> changedProperties = TransactionDataParser.getChangedProperties(propertyChangeEvent);

            changedProperties.forEach((k,v)-> {
                System.out.println(k);
                System.out.println(v);
            });
//            for (Map<String, JSONObject> txEvent : txEvents) {
//
//                System.out.println(txEvent.get("NodePropertyChange").toString());
//
//            }



//            String[] deletedPropertyKeys = TransactionDataParser.getRemovedProperties(txEvents)
//;
//
//            Assertions.assertEquals(2,allProperties.length());
//            Assertions.assertEquals( "foo",allProperties.get("testProperty"));






        } catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

}
