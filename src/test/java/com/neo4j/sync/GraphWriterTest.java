package com.neo4j.sync;


import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionDataParser;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;


@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ImpermanentDbmsExtension
public class GraphWriterTest {
    private static final String ADD_NODE = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private static final String LOCAL_lABEL = "LocalTx";
    private static final String TEST_REL_TYPE = "CONNECTED_TO";
    private static final String TEST_REL_TYPE2 = "LIKES";
    private final String ADD_NODES_AND_PROPERTIES = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_RELATIONSHIP = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_PROPERTIES_TO_REL = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{\"weight\":123},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":null}],\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE2 = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":\"bar\"}],\"allProperties\":{\"test\":\"bar\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String REL_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"RelationPropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":[{\"propertyName\":\"weight\",\"oldValue\":1,\"newValue\":2}],\"allProperties\":{\"weight\":2},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    @Inject
    public GraphDatabaseAPI graphDatabaseAPI;

    @Test
    void addNodeTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();
    }

    @Test
    void addNodeTest2() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_NODES_AND_PROPERTIES);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        ResourceIterator<Node> txRecordNodes = tx.findNodes(Label.label("TransactionRecord"));
        assertFalse(txRecordNodes.hasNext());

        tx.commit();
    }

    @Test
    void addNodesAndRelationshipTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_NODES_AND_RELATIONSHIP);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        newNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName((TEST_REL_TYPE)))));
        ResourceIterator<Node> txRecordNodes = tx.findNodes(Label.label("TransactionRecord"));
        assertFalse(txRecordNodes.hasNext());
        tx.commit();
    }

    @Test
    void addNodesAndRelationshipsTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_MULTIPLE_RELATIONSHIPS);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));

        for (Node node : newNodes) {
            if (node.getDegree() > 1) {
                assertTrue(node.hasRelationship(RelationshipType.withName((TEST_REL_TYPE2))));
            }
        }
        tx.commit();
    }

    @Test
    void addNodeAndPropertiesTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();
    }

    @Test
    void addPropertiesToRelTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_PROPERTIES_TO_REL);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label("Test"), "uuid", "123XYZ");
        assertTrue(newNodes.iterator().hasNext());
        for (Node node : newNodes) {
            Relationship relationship = node.getSingleRelationship(RelationshipType.withName("CONNECTED_TO"), Direction.OUTGOING);
            assertTrue(relationship.hasProperty("weight"));
            assertEquals("123", relationship.getProperty("weight").toString());
        }
        // check to see if the updated property and value exist
        tx.commit();
    }


    @Test
    void relPropertyChangeTest1() throws Exception {
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(ADD_NODES_AND_RELATIONSHIP);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        graphTxTranslation = TransactionDataParser.translateTransactionData(REL_PROPERTY_CHANGE);
        graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        for (Node node : newNodes) {
            Relationship relationship = node.getSingleRelationship(RelationshipType.withName("CONNECTED_TO"), Direction.BOTH);
            assertTrue(relationship.hasProperty("weight"));
            assertEquals("2", relationship.getProperty("weight").toString());
        }
        // check to see if the relationships have the right properties and values.
        tx.commit();
    }

    @Test
    void nodePropertyChangeTest1() throws Exception {
        assertNotNull(graphDatabaseAPI);
        assertNotNull(graphDatabaseAPI);
        Transaction tx1 = graphDatabaseAPI.beginTx();
        Node myNode = tx1.createNode(Label.label("LocalTx"));
        myNode.addLabel(Label.label("Test"));
        myNode.setProperty("uuid", "123XYZ");
        myNode.setProperty("test", "foo");
        tx1.commit();
        tx1.close();
        JSONObject graphTxTranslation = TransactionDataParser.translateTransactionData(NODE_PROPERTY_CHANGE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI, mock(Log.class));
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertTrue(newNodes.iterator().hasNext());
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        for (Node node : newNodes) {
            assertFalse(node.hasProperty("test"));
        }
        // check to see if the relationships have the right properties and values.
        tx.commit();
    }
}







