package com.neo4j.sync.engine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.logging.Log;

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
    private static final String UUID = "uuid";
    private static final String TRANSACTION_RECORD = "TransactionRecord";
    private static final String LAST_TRANSACTION_REPLICATED = "LastTransactionReplicated";

    private ReplicationJudge() {
        // Private constructor to hide implicit public one
    }

    public static boolean approved(TransactionData data, Log log) {

        if (log.isDebugEnabled()) {
            logTransactionData(data, log);
        }

        if (data.createdNodes().iterator().hasNext() && !data.assignedLabels().iterator().hasNext()) {
            log.debug("ReplicationJudge -> nodes were created without labels: Skip");
            return false;
        }
        if (data.assignedLabels().iterator().hasNext()) {

            Iterable<LabelEntry> labels = data.assignedLabels();
            for (LabelEntry le : labels)
                if (le.label().name().equals(LOCAL_TX) || le.label().name().equals(DO_NOT_REPLICATE) ||
                        le.label().name().equals(TRANSACTION_RECORD)) {
                    log.debug("ReplicationJudge -> an assigned label either was LocaTx or DoNotReplicate: Skip");
                    return false;
                }
        }

        if (data.createdNodes().iterator().hasNext() && !data.assignedNodeProperties().iterator().hasNext()) {
            log.debug("ReplicationJudge -> nodes were created without properties : Skip");
            return false;
        }

        if (data.deletedRelationships().iterator().hasNext()) {

            Iterable<Relationship> rels = data.deletedRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) {
                    log.debug("ReplicationJudge -> relationships were deleted but were attached to nodes with LocalTX label : Skip");
                    return false;
                }
            }
        }
        if (data.deletedNodes().iterator().hasNext()) {
            Iterable<Node> nodes = data.deletedNodes();
            for (Node node : nodes) {
                if (node.hasLabel(Label.label(LOCAL_TX))) {
                    log.debug("ReplicationJudge -> nodes were deleted but had LocalTx label: Skip");
                    return false;
                }
            }
        }

        if (data.createdRelationships().iterator().hasNext()) {
            Iterable<Relationship> rels = data.createdRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) {
                    log.debug("ReplicationJudge -> relationships were created but were attached to nodes with LocalTX label: Skip");
                    return false;
                }

            }
        }

        if (data.assignedRelationshipProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Relationship>> relPropEntry = data.assignedRelationshipProperties();

            for (PropertyEntry<Relationship> pe : relPropEntry) {

                if (pe.entity().getStartNode().hasLabel(Label.label(LOCAL_TX)) || !pe.entity().hasProperty(UUID)) {
                    log.debug("ReplicationJudge -> relationship properties were assigned to nodes with LocalTX label, " +
                            "or uuid was assigned to relationship : Skip");
                    return false;
                }

            }

        }

        if (data.assignedNodeProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Node>> nodePropEntry = data.assignedNodeProperties();

            for (PropertyEntry<Node> ne : nodePropEntry) {
                if (!ne.entity().hasProperty(UUID)) {
                    log.debug("ReplicationJudge -> no uuid was assigned to node: Skip");
                    return false;
                } else {
                    if (lastTransactionReplicatedNodeLabelExists(ne.entity().getLabels(), log)) {
                        return false;
                    }
                }
            }
        }

        log.debug("ReplicationJudge -> Validation passed. Replicating transaction.");
        return true;
    }

    private static boolean lastTransactionReplicatedNodeLabelExists(Iterable<Label> labels, Log log) {
        if (labels.iterator().hasNext()) {
            for (Label label : labels) {
                if (label.name().equals(LAST_TRANSACTION_REPLICATED)) {
                    log.debug("Transaction counter. Ignore and skip replication.");
                    return true;
                }
            }
        }
        return false;
    }

    private static void logTransactionData(TransactionData data, Log log) {

        if (data.assignedLabels().iterator().hasNext()) {
            for (LabelEntry labelEntry : data.assignedLabels()) {
                log.debug("LABEL: " + labelEntry.label().name());
            }
        } else {
            log.debug("No assigned labels");
        }

        if (data.createdNodes().iterator().hasNext()) {
            for (Node node : data.createdNodes()) {
                printNodeData(node, log);
            }
        } else {
            log.debug("No created nodes");
        }

        if (data.assignedNodeProperties().iterator().hasNext()) {
            for (PropertyEntry<Node> propertyEntry : data.assignedNodeProperties()) {
                log.debug("PROPERTY: " + propertyEntry.key() + " | " + propertyEntry.value());
                log.debug("HAS uuid PROPERTY: " + propertyEntry.entity().hasProperty(UUID));

                if (propertyEntry.entity().getLabels().iterator().hasNext()) {
                    for (Label label : propertyEntry.entity().getLabels()) {
                        log.debug("Property Entity LABEL: " + label.name());
                    }
                } else {
                    log.debug("No labels for property root entity");
                }
            }
        } else {
            log.debug("No assigned node properties");
        }

        if (!data.metaData().isEmpty()) {
            for (Map.Entry<String, Object> entry : data.metaData().entrySet()) {
                log.debug("Metadata: " + entry.getKey() + " | " + entry.getValue());
            }
        } else {
            log.debug("No metadata");
        }

    }

    private static void printNodeData(Node node, Log log) {
        log.debug("NODE ID: " + node.getId());

        log.debug("NODE Labels: ");
        for (Label label : node.getLabels()) {
            log.debug("%s", label);
        }
        log.debug("NODE properties: %s", node.getAllProperties().toString());
    }

}


