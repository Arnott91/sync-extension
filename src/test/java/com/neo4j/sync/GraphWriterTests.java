package com.neo4j.sync;


import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionDataParser;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionDataParser;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import org.codehaus.jettison.json.JSONObject;

import static org.junit.jupiter.api.Assertions.*;


@TestInstance( TestInstance.Lifecycle.PER_METHOD )
@ImpermanentDbmsExtension
public class GraphWriterTests {
    private static final String ADD_NODE = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";;
    private final String ADD_NODES_AND_PROPERTIES = "{\"transactionEvents\":[{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"foo\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XZY123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"testProperty\":\"bar\",\"uuid\":\"XZY123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_NODES_AND_RELATIONSHIP = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_MULTIPLE_RELATIONSHIPS = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"LIKES\",\"targetNodeLabels\":[\"Movie\"],\"targetPrimaryKey\":{\"uuid\":\"ABC\"},\"properties\":null,\"allProperties\":{},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Movie\"],\"primaryKey\":{\"uuid\":\"ABC\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"ABC\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String ADD_PROPERTIES_TO_REL = "{\"transactionEvents\":[{\"changeType\":\"AddRelation\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":null,\"allProperties\":{\"weight\":123},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null},{\"changeType\":\"AddNode\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"XYZ123\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":null,\"allProperties\":{\"uuid\":\"XYZ123\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":null}],\"allProperties\":{\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String NODE_PROPERTY_CHANGE2 = "{\"transactionEvents\":[{\"changeType\":\"NodePropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":null,\"targetNodeLabels\":null,\"targetPrimaryKey\":null,\"properties\":[{\"propertyName\":\"test\",\"oldValue\":\"foo\",\"newValue\":\"bar\"}],\"allProperties\":{\"test\":\"bar\",\"uuid\":\"123XYZ\"},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private final String REL_PROPERTY_CHANGE = "{\"transactionEvents\":[{\"changeType\":\"RelationPropertyChange\",\"nodeLabels\":[\"Test\"],\"primaryKey\":{\"uuid\":\"123XYZ\"},\"nodeKey\":null,\"relationshipLabel\":\"CONNECTED_TO\",\"targetNodeLabels\":[\"Test\"],\"targetPrimaryKey\":{\"uuid\":\"XYZ123\"},\"properties\":[{\"propertyName\":\"weight\",\"oldValue\":1,\"newValue\":2}],\"allProperties\":{\"weight\":2},\"uuid\":null,\"timestamp\":null,\"transactionId\":null,\"targetNodeKey\":null}]}";
    private static final String LOCAL_lABEL = "com.neo4j.sync.engine.LocalTx";


    @Inject
    public GraphDatabaseAPI graphDatabaseAPI;

    public GraphWriterTests() throws JSONException {
    }


    @Test
    void addNodeTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void addNodeTest2() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void addNodesAndRelationshipTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODES_AND_RELATIONSHIP);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void addNodesAndRelationshipsTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_MULTIPLE_RELATIONSHIPS);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void addNodeAndPropertiesTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void addPropertiesToRelTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_PROPERTIES_TO_REL);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void nodePropertyChangeTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void nodePropertyChangeTest2() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(NODE_PROPERTY_CHANGE2);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }

    @Test
    void relPropertyChangeTest1() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(REL_PROPERTY_CHANGE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }


    @Test
    void relPropertyChangeTest2() throws Exception
    {
        assertNotNull(graphDatabaseAPI);
        JSONObject graphTxTranslation = TransactionDataParser.TranslateTransactionData(ADD_NODE);
        GraphWriter graphWriter = new GraphWriter(graphTxTranslation, graphDatabaseAPI);
        graphWriter.executeCRUDOperation();
        Transaction tx = graphDatabaseAPI.beginTx();
        Iterable<Node> newNodes  = () -> tx.findNodes(Label.label(LOCAL_lABEL));
        assertNotNull(newNodes);
        newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label(LOCAL_lABEL))));
        tx.commit();


    }



}







