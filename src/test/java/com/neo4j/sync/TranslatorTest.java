package com.neo4j.sync;

import com.neo4j.sync.engine.TransactionDataParser;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.test.extension.Inject;

import java.util.List;
import java.util.Map;

@TestInstance( TestInstance.Lifecycle.PER_METHOD )

public class TranslatorTest {

    @Inject
    private final String ADD_NODE = "[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]";
    private final String ADD_NODES_AND_PROPERTIES = "[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]";
    private final String ADD_NODES_AND_RELATIONSHIP = "[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]";
    private final String ADD_PROPERTIES_TO_REL = "[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{\"weight\":123},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]";

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

}
