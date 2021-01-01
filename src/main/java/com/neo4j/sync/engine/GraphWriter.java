package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import java.util.List;
import java.util.Map;

/**
 * com.neo4j.sync.engine.GraphWriter is used primarily as a testing class to write atomic event-based transactions
 * provided in JSON by the TransactionDataParser.  The functionality of this class is replaced by the
 * TransacctionDataHandler, which executes one transaction for all events in the original transaction,
 * as opposed to a transaction per event (add_node, delete_node, etc..).
 *
 * @author Chris Upkes
 */

public class GraphWriter {

    public static final String ADD_NODE = "AddNode";
    public static final String DELETE_NODE = "DeleteNode";
    public static final String ADD_PROPERTY = "AddProperty";
    public static final String NODE_PROPERTY_CHANGE = "NodePropertyChange";
    public static final String ADD_RELATION = "AddRelation";
    public static final String DELETE_RELATION = "DeleteRelation";
    public static final String RELATIONSHIP_PROPERTY_CHANGE = "RelationPropertyChange";
    private final List<Map<String, JSONObject>> transactionEvents;
    private final GraphDatabaseAPI graphDb;
    private Log log;

    public GraphWriter(JSONObject graphTransaction, GraphDatabaseService graphDb, Log log) throws JSONException {

        // split the transactionEvents JSON into a list of separate events

        this.transactionEvents = TransactionDataParser.getTransactionEvents(graphTransaction);
        this.graphDb = (GraphDatabaseAPI) graphDb;
        this.log = log;
    }

    public GraphWriter(String graphTransaction, GraphDatabaseService graphDb, Log log) throws JSONException {

        // split the transactionEvents JSON into a list of separate events

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(graphTransaction));
        this.graphDb = (GraphDatabaseAPI) graphDb;
        this.log = log;
    }

    public GraphWriter(String graphTransaction, GraphDatabaseService graphDb) throws JSONException {

        // split the transactionEvents JSON into a list of separate events

        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(graphTransaction));
        this.graphDb = (GraphDatabaseAPI) graphDb;

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
                   case NODE_PROPERTY_CHANGE:
                        this.delegateCRUDOperation(v, ChangeType.NODE_PROPERTY_CHANGE);
                        break;
                    case ADD_RELATION:
                        this.delegateCRUDOperation(v, ChangeType.ADD_RELATION);
                        break;
                    case DELETE_RELATION:
                        this.delegateCRUDOperation(v, ChangeType.DELETE_RELATION);
                        break;
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
            case RELATION_PROPERTY_CHANGE:
                this.changeRelationProperties(event);
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


        try (Transaction tx = graphDb.beginTx()) {
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
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("changeRelationProperties tx complete");
            //og.info("proc complete");
        }
    }





    private void addRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);

        //Map<String, String> properties = TransactionDataParser.getRelationProperties(event);
        Map<String, Object> properties = TransactionDataParser.getRelationProperties(event);


        try (Transaction tx = graphDb.beginTx()) {
            // first try and find the nodes.  If they don't exist we must create them.
            Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0].toString(), startPrimaryKey[1].toString());
            Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0].toString(), targetPrimaryKey[1].toString());
            Relationship relationshipFrom = startNode.createRelationshipTo(targetNode, RelationshipType.withName(TransactionDataParser.getRelationType(event)));
            if (properties.size() > 0) properties.forEach(relationshipFrom::setProperty);
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("addRelation tx complete");
            //og.info("proc write complete");
        }
    }

    private void deleteRelation(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label startSearchLabel = finder.getSearchLabel(NodeDirection.START);
        Label targetSearchLabel = finder.getSearchLabel(NodeDirection.TARGET);

        String[] startPrimaryKey = finder.getPrimaryKey(NodeDirection.START);
        String[] targetPrimaryKey = finder.getPrimaryKey(NodeDirection.TARGET);

        try (Transaction tx = graphDb.beginTx()) {
            Node startNode = tx.findNode(startSearchLabel, startPrimaryKey[0], startPrimaryKey[1]);
            Node targetNode = tx.findNode(targetSearchLabel, targetPrimaryKey[0], targetPrimaryKey[1]);

            for (Relationship relationship : startNode.getRelationships(Direction.OUTGOING, RelationshipType.withName(TransactionDataParser.getRelationType(event)))) {
                if (relationship.getEndNode().equals(targetNode)) relationship.delete();
            }
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("deleteRelation tx complete");
            //og.info("proc write complete");
        }
    }

    private void deleteNodes(JSONObject event) throws JSONException {

        NodeFinder finder = new NodeFinder(event);
        Label searchLabel = finder.getSearchLabel();
        String[] primaryKey = finder.getPrimaryKey();

        try (Transaction tx = graphDb.beginTx()) {
            Node foundNode = tx.findNode(searchLabel, primaryKey[0], primaryKey[1]);
            foundNode.delete();
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("deleteNodes tx complete");
            //og.info("proc write complete");
        }
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

        try (Transaction tx = graphDb.beginTx()) {
            Node foundNode = tx.findNode(searchLabel, primaryKey[0], primaryKey[1]);

            for (String removedPropertyKey : removedPropertyKeys) {
                foundNode.removeProperty(removedPropertyKey);
            }

            for (Map.Entry<String, Object> entry : changedProperties.entrySet()) {

                foundNode.setProperty(entry.getKey(), entry.getValue());
            }
            tx.commit();

        } catch (Exception e) {
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("changeNodeProperties tx complete");
            //og.info("proc write complete");
        }
    }


    private void addNode(JSONObject event) throws JSONException {

        // get the array of labels
        String[] labels = (String[]) TransactionDataParser.getNodeLabels(event);
        // get the collection of properties
        Map<String,Object> properties = TransactionDataParser.getNodeProperties(event);


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
            e.printStackTrace();
            ///log.error(e.getMessage(), e);
        } finally {
            System.out.println("addNode tx complete");
            //og.info("proc write complete");
        }
    }
}
