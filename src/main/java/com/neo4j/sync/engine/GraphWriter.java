package com.neo4j.sync.engine;


import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.*;
import scala.util.parsing.json.JSON;

public class GraphWriter {

    private final List<Map<String, JSONObject>> transactionEvents;
    private String changeType;
    private  GraphDatabaseAPI graphDb;
    private Log log;

    public GraphWriter(JSONObject graphTransaction, GraphDatabaseService graphDb) throws JSONException {

        // split the transactionEvents JSON into a list of separate events

        this.transactionEvents = TransactionDataParser.getTransactionEvents(graphTransaction);

    }

    public void executeCRUDOperation(List<Map<String, JSONObject>> events) throws JSONException {

        // logic to loop through all events in the event array and determine change type and then call delegate.

        for (Map<String, JSONObject> entry : events){
            for (Map.Entry<String, JSONObject> e : entry.entrySet()) {
                String k = e.getKey();
                JSONObject v = e.getValue();
                switch (k) {
                    case "addNode":
                        this.delegateCRUDOperation(v,ChangeType.ADD_NODE);
                    case "deleteNode":
                        this.delegateCRUDOperation(v,ChangeType.DELETE_NODE);
                    case "addProperty":
                        this.delegateCRUDOperation(v,ChangeType.ADD_PROPERTY);
                    case "removeProperty":
                        this.delegateCRUDOperation(v,ChangeType.REMOVE_PROPERTY);
                    case "addRelation":
                        this.delegateCRUDOperation(v,ChangeType.ADD_RELATION);
                    case "deleteRelation":
                        this.delegateCRUDOperation(v,ChangeType.DELETE_RELATION);
                    case "addRelationProperty":
                        this.delegateCRUDOperation(v,ChangeType.ADD_RELATION_PROPERTY);
                    case "removeRelationProperty":
                        this.delegateCRUDOperation(v,ChangeType.REMOVE_RELATION_PROPERTY);
                    default:
                }

            }

        }

    }

    private void delegateCRUDOperation(JSONObject event, ChangeType changeType) throws JSONException {

        switch (changeType) {
            case ADD_NODE: this.addNode(event);
            case ADD_PROPERTY: this.addProperties(event);
            case REMOVE_PROPERTY: this.removeProperties(event);
            case DELETE_NODE: this.deleteNodes(event);
            case DELETE_RELATION: this.deleteRelation(event);
            case ADD_RELATION: this.addRelation(event);
            case ADD_RELATION_PROPERTY: this.addRelationProperties(event);
            case REMOVE_RELATION_PROPERTY: this.removeRelationProperties(event);
        }

    }

    private void removeRelationProperties(JSONObject event) {
    }

    private void addRelationProperties(JSONObject event) {
    }

    private void addRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);
        Map<String, String> properties = TransactionDataParser.getRelationProperties(event);




        try (Transaction tx = graphDb.beginTx()) {
            Node startNode = tx.findNode(startSearchLabel,startPrimaryKey[0],startPrimaryKey[1]);
            Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0],targetPrimaryKey[1]);
            startNode.createRelationshipTo(targetNode,RelationshipType.withName(TransactionDataParser.getRelationType(event)));
            Relationship singleRelationship = startNode.getSingleRelationship(RelationshipType.withName(TransactionDataParser.getRelationType(event)), Direction.OUTGOING);
            if (properties.size() > 0) properties.forEach(singleRelationship::setProperty);


            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }

    private void deleteRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);

        try (Transaction tx = graphDb.beginTx()) {
            Node startNode = tx.findNode(startSearchLabel,startPrimaryKey[0],startPrimaryKey[1]);
            Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0],targetPrimaryKey[1]);
            for (Relationship relationship : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(TransactionDataParser.getRelationType(event)))) {
                relationship.delete();
            }
            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }

    private void deleteNodes(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();

        try (Transaction tx = graphDb.beginTx()) {
            Node foundNode = tx.findNode(searchLabel,primaryKey[0],primaryKey[1]);
            foundNode.delete();
            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }

    private void removeProperties(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();
        Map<String, String> properties = TransactionDataParser.getNodeProperties(event);
        // we need the primary key to find the node

        try (Transaction tx = graphDb.beginTx()) {
            Node foundNode = tx.findNode(searchLabel,primaryKey[0],primaryKey[1]);
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                String k = entry.getKey();
                String v = entry.getValue();
                foundNode.removeProperty(k);
            }
            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }

    private void addProperties(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();

        // get the collection of properties
        Map<String, String> properties = TransactionDataParser.getNodeProperties(event);





        try (Transaction tx = graphDb.beginTx()) {

            Node foundNode = tx.findNode(searchLabel,primaryKey[0],primaryKey[1]);
            properties.forEach(foundNode::setProperty);


            tx.commit();
        } catch (Exception e) {

            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }

    private void addNode(JSONObject event) throws JSONException {

        // get the array of labels
        String[] labels = TransactionDataParser.getNodeLabels(event);
        // get the collection of properties
        Map<String, String> properties = TransactionDataParser.getNodeProperties(event);


        try (Transaction tx = graphDb.beginTx()) {
            Node newNode = tx.createNode();

            // add labels
            for (String label : labels) {
                newNode.addLabel(Label.label(label));
            }
            // add properties
            properties.forEach(newNode::setProperty);

            tx.commit();
        } catch (Exception e) {
            // log exception
            //this.logException(e, databaseService);
            System.out.println(e.getMessage());

        } finally
        {
            log.info("proc write complete");


        }
    }








}
