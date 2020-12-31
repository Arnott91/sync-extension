package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;


public class TransactionDataHandler {

    public static final String ADD_NODE = "AddNode";
    public static final String DELETE_NODE = "DeleteNode";
    public static final String ADD_PROPERTY = "AddProperty";
    public static final String NODE_PROPERTY_CHANGE = "NodePropertyChange";
    public static final String ADD_RELATION = "AddRelation";
    public static final String DELETE_RELATION = "DeleteRelation";
    public static final String ADD_RELATION_PROPERTY = "AddRelationProperty";
    public static final String REMOVE_RELATION_PROPERTY = "RemoveRelationProperty";
    public static final String RELATIONSHIP_PROPERTY_CHANGE = "RelationPropertyChange";
    private final List<Map<String, JSONObject>> transactionEvents;
    private Transaction tx;
    private Log log;


    public TransactionDataHandler(String transactionData, Transaction tx) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(transactionData));
        this.tx = tx;
    }

    public TransactionDataHandler(String transactionData, List<Map<String, JSONObject>> transactionEvents, Transaction tx, Log log) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(transactionData));
        this.log = log;
        this.tx = tx;
    }

    public TransactionDataHandler(JSONObject transactionData, Transaction tx) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(transactionData);
        this.tx = tx;
    }

    public TransactionDataHandler(JSONObject transactionData, Transaction tx, Log log) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(transactionData);
        this.log = log;
        this.tx = tx;
    }



    public void executeCRUDOperation() throws JSONException {

        // logic to loop through all events in the event array and determine change type and then call delegate.

        for (Map<String, JSONObject> entry : transactionEvents) {
            for (Map.Entry<String, JSONObject> e : entry.entrySet()) {
                String k = e.getKey();
                JSONObject v = e.getValue();
                switch (k) {
                    case ADD_NODE:
                        this.delegateCRUDOperation(v, ChangeType.ADD_NODE);
                        break;
                    case DELETE_NODE:
                        this.delegateCRUDOperation(v, ChangeType.DELETE_NODE);
                        break;
                    case ADD_PROPERTY:
                        this.delegateCRUDOperation(v, ChangeType.ADD_PROPERTY);
                        break;
                    case NODE_PROPERTY_CHANGE:
                        this.delegateCRUDOperation(v, ChangeType.NODE_PROPERTY_CHANGE);
                        break;
                    case ADD_RELATION:
                        this.delegateCRUDOperation(v, ChangeType.ADD_RELATION);
                        break;
                    case DELETE_RELATION:
                        this.delegateCRUDOperation(v, ChangeType.DELETE_RELATION);
                        break;
                    case ADD_RELATION_PROPERTY:
                        this.delegateCRUDOperation(v, ChangeType.ADD_RELATION_PROPERTY);
                        break;
                    case REMOVE_RELATION_PROPERTY:
                    case RELATIONSHIP_PROPERTY_CHANGE:
                        this.delegateCRUDOperation(v, ChangeType.RELATION_PROPERTY_CHANGE);
                        break;


                }

            }

        }

    }

    private void delegateCRUDOperation(JSONObject event, ChangeType changeType) throws JSONException {

        switch (changeType) {
            case ADD_NODE:
                this.addNode(event);
                break;
            case ADD_PROPERTY:
                this.addProperties(event);
                break;
            case NODE_PROPERTY_CHANGE:
                this.changeNodeProperties(event);
                break;
            case DELETE_NODE:
                this.deleteNodes(event);
                break;
            case DELETE_RELATION:
                this.deleteRelation(event);
                break;
            case ADD_RELATION:
                this.addRelation(event);
                break;
            case ADD_RELATION_PROPERTY:
                this.addRelationProperties(event);
                break;
            case RELATION_PROPERTY_CHANGE:
                this.changeRelationProperties(event);
        }
    }

    private void addNode(JSONObject event) throws JSONException {

        // get the array of labels
        String[] labels = (String[]) TransactionDataParser.getNodeLabels(event);
        // get the collection of properties
        Map<String, Object> properties = TransactionDataParser.getNodeProperties(event);

        Node newNode = tx.createNode();

        // add labels
        for (String label : labels) {
            newNode.addLabel(Label.label(label));
        }
        // add properties
        properties.forEach(newNode::setProperty);


    }

    private void deleteNodes(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();

        Node foundNode = tx.findNode(searchLabel, primaryKey[0], primaryKey[1]);
        foundNode.delete();


    }

    private void addRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);

        //Map<String, String> properties = TransactionDataParser.getRelationProperties(event);
        Map<String, Object> properties = TransactionDataParser.getRelationProperties(event);

        // first try and find the nodes.  If they don't exist we must create them.
        Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0].toString(), startPrimaryKey[1].toString());

        Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0].toString(), targetPrimaryKey[1].toString());

        Relationship relationshipFrom = startNode.createRelationshipTo(targetNode, RelationshipType.withName(TransactionDataParser.getRelationType(event)));
        if (properties.size() > 0) properties.forEach(relationshipFrom::setProperty);



    }

    private void deleteRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);


        Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0], startPrimaryKey[1]);
        Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0], targetPrimaryKey[1]);

        for (Relationship relationship : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(TransactionDataParser.getRelationType(event)))) {
            if (relationship.getEndNode().equals(targetNode)) relationship.delete();
        }


    }

    private void addProperties(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();

        // get the collection of properties
        Map<String, Object> properties = TransactionDataParser.getNodeProperties(event);

        Node foundNode = tx.findNode(searchLabel, primaryKey[0], primaryKey[1]);
        properties.forEach(foundNode::setProperty);


    }

    private void changeNodeProperties(JSONObject event) throws JSONException {

        // not very elegant, but works.  Might be able to make a little less verbose
        // without being cryptic.

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();
        Map<String, Object> changedProperties = TransactionDataParser.getChangedProperties(event);
        String[] removedPropertyKeys = TransactionDataParser.getRemovedProperties(event);
        // we need the primary key to find the node

        Node foundNode = tx.findNode(searchLabel, primaryKey[0], primaryKey[1]);

        for (String removedPropertyKey : removedPropertyKeys) {
            foundNode.removeProperty(removedPropertyKey);
        }

        for (Map.Entry<String, Object> entry : changedProperties.entrySet()) {

            foundNode.setProperty(entry.getKey(), entry.getValue());
        }


    }

    private void addRelationProperties(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);

        Map<String,Object> properties = TransactionDataParser.getRelationProperties(event);
        Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0], startPrimaryKey[1]);
        Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0], targetPrimaryKey[1]);
        Relationship singleRelationship = startNode.getSingleRelationship(RelationshipType.withName(TransactionDataParser.getRelationType(event)), Direction.OUTGOING);
        if (properties.size() > 0) {
            properties.forEach(singleRelationship::setProperty);
        }


    }

    private void changeRelationProperties(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);
        Map<String, Object> properties = TransactionDataParser.getChangedProperties(event);
        String[] removedProperties = TransactionDataParser.getRemovedProperties(event);

        Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0], startPrimaryKey[1]);
        Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0], targetPrimaryKey[1]);
        Relationship singleRelationship = startNode.getSingleRelationship(RelationshipType.withName(TransactionDataParser.getRelationType(event)), Direction.OUTGOING);
        // make sure it's the relationship between the start and target nodes.

        if (singleRelationship.getEndNode().equals(targetNode)) {

            for (String removedProperty : removedProperties) {
                singleRelationship.removeProperty(removedProperty);
            }
            if (properties.size() > 0) properties.forEach(singleRelationship::setProperty);
        }

    }





}
