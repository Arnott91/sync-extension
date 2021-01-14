package com.neo4j.sync.engine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.util.Map;

/**
 * com.neo4j.sync.engine.ReplicationJudge class is used to determine whether the transaction we are intercepting
 * should be replicated.
 *
 * @author Chris Upkes
 */
public class ReplicationJudge {

    private static final String LOCAL_TX = "LocalTx";
    private static final String DO_NOT_REPLICATE = "DoNotReplicate";

    // if you must defy Neo4j coding conventions and define properties with uppercase names then
    //TODO: consider either changing properties to lowercase or change all uuid references to UUID.
    private static final String UUID = "uuid";
    private static final String TRANSACTION_RECORD = "TransactionRecord";
    private static final String LAST_TRANSACTION_REPLICATED = "LastTransactionReplicated";


    // if you must defy Neo4j coding conventions and define properties with uppercase names then
    //TODO: consider either changing properties to lowercase or change all uuid references to UUID.

    public static boolean approved(TransactionData data) {

        logTransactionForTesting(data);

        if (data.createdNodes().iterator().hasNext() && !data.assignedLabels().iterator().hasNext()) {
            System.out.println("nodes were created without labels: NAY");
            return false;
        }
        if (data.assignedLabels().iterator().hasNext()) {

            Iterable<LabelEntry> labels = data.assignedLabels();
            for (LabelEntry le : labels)
                if (le.label().name().equals(LOCAL_TX) || le.label().name().equals(DO_NOT_REPLICATE) ||
                        le.label().name().equals(TRANSACTION_RECORD)) {
                    System.out.println("an assigned label either was LocaTx or DoNotReplicate: NAY");
                    return false;
                }
        }

        if (data.createdNodes().iterator().hasNext() && !data.assignedNodeProperties().iterator().hasNext()) {
            System.out.println("nodes were created without properties : NAY");
            return false;
        }

        if (data.deletedRelationships().iterator().hasNext()) {

            Iterable<Relationship> rels = data.deletedRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) {
                    System.out.println("relationships were deleted but were attached to nodes with LocalTX Label : NAY");
                    return false;
                }
            }
        }
        if (data.deletedNodes().iterator().hasNext()) {
            Iterable<Node> nodes = data.deletedNodes();
            for (Node node : nodes) {
                if (node.hasLabel(Label.label(LOCAL_TX))) {
                    System.out.println("nodes were deleted but had LocalTx label: NAY");
                    return false;
                }
            }
        }

        if (data.createdRelationships().iterator().hasNext()) {
            Iterable<Relationship> rels = data.createdRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) {
                    System.out.println("relationships were created but were attached to nodes with LocalTX Label: NAY");
                    return false;
                }

            }
        }

        if (data.assignedRelationshipProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Relationship>> relPropEntry = data.assignedRelationshipProperties();

            for (PropertyEntry<Relationship> pe : relPropEntry) {

                if (pe.entity().getStartNode().hasLabel(Label.label(LOCAL_TX)) || !pe.entity().hasProperty(UUID)) {
                    System.out.println("relationship properties were assigned to nodes with LocalTX label : NAY\n");
                    System.out.println(" or uuid was assigned to relationship");
                    return false;
                }

            }

        }

        if (data.assignedNodeProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Node>> nodePropEntry = data.assignedNodeProperties();

            for (PropertyEntry<Node> ne : nodePropEntry) {
                if (!ne.entity().hasProperty(UUID)) {
                    System.out.println("no uuid was assigned to node");
                    return false;
                } else {
                    if (lastTransactionReplicatedNodeLabelExists(ne.entity().getLabels())) {
                        return false;
                    }
                }
            }
        }
        System.out.println("ReplicationJudge validation passed; returning TRUE");
        return true;
    }

    private static boolean lastTransactionReplicatedNodeLabelExists(Iterable<Label> labels) {
        if (labels.iterator().hasNext()) {
            for (Label label : labels) {
                if (label.name().equals(LAST_TRANSACTION_REPLICATED)) {
                    System.out.println("Transaction counter. Ignore and skip replication.");
                    return true;
                }
            }
        }
        return false;
    }

    private static void logTransactionForTesting(TransactionData data) {

        if (data.assignedLabels().iterator().hasNext()) {
            for (LabelEntry labelEntry : data.assignedLabels()) {
                System.out.println("LABEL: " + labelEntry.label().name());
            }
        } else {
            System.out.println("NO LABELS BEING PICKED UP!!!!!");
        }

        if (data.createdNodes().iterator().hasNext()) {
            for (Node node : data.createdNodes()) {
                printNodeData(node);
            }
        } else {
            System.out.println("No created nodes");
        }

        if (data.assignedNodeProperties().iterator().hasNext()) {
            for (PropertyEntry<Node> propertyEntry : data.assignedNodeProperties()) {
                System.out.println("PROPERTY: " + propertyEntry.key() + " | " + propertyEntry.value());
                System.out.println("HAS uuid PROPERTY: " + propertyEntry.entity().hasProperty(UUID));

                if (propertyEntry.entity().getLabels().iterator().hasNext()) {
                    for (Label label : propertyEntry.entity().getLabels()) {
                        System.out.println("Property Entity LABEL: " + label.name());
                    }
                } else {
                    System.out.println("No labels for property root entity!");
                }
            }
        } else {
            System.out.println("No assigned node properties");
        }

        if (!data.metaData().isEmpty()) {
            for (Map.Entry<String, Object> entry : data.metaData().entrySet()) {
                System.out.println("Metadata: " + entry.getKey() + " | " + entry.getValue());
            }
        } else {
            System.out.println("No metadata");
        }

    }

    private static void printNodeData(Node node) {
        System.out.println("NODE ID: " + node.getId());

        System.out.println("NODE Labels: ");
        for (Label label : node.getLabels()) {
            System.out.println(label);
        }
        System.out.println("NODE properties: " + node.getAllProperties().toString());
    }

}


