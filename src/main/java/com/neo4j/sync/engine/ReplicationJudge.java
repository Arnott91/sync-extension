package com.neo4j.sync.engine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

/**
 * com.neo4j.sync.engine.ReplicationJudge class is used to determine whether the transaction we are intercepting
 * should be replicated.
 *
 * @author Chris Upkes
 */
public class ReplicationJudge {

    public static final String LOCAL_TX = "LocalTx";
    public static final String DO_NOT_REPLICATE = "DoNotReplicate";

    // if you must defy Neo4j coding conventions and define properties with uppercase names then
    //TODO: consider either changing properties to lowercase or change all uuid references to UUID.
    public static final String UUID = "uuid";
    private static final boolean NAY = false;
    private static final boolean YEA = true;

    // if you must defy Neo4j coding conventions and define properties with uppercase names then
    //TODO: consider either changing properties to lowercase or change all uuid references to UUID.

    public static boolean approved(TransactionData data) {

        boolean votes = YEA;
        if (data.createdNodes().iterator().hasNext() && !data.assignedLabels().iterator().hasNext()) {
            votes = NAY;
            System.out.println("nodes were created without labels: NAY");
        }
        if (data.assignedLabels().iterator().hasNext()) {

            Iterable<LabelEntry> labels = data.assignedLabels();
            for (LabelEntry le : labels)
                if (le.label().name().equals(LOCAL_TX) || le.label().name().equals(DO_NOT_REPLICATE))
                    votes = votes & NAY;
            if (votes == NAY) System.out.println("an assigned label either was LocaTx or DoNotReplicate: NAY");
        }

        if (data.createdNodes().iterator().hasNext() && !data.assignedNodeProperties().iterator().hasNext()) {
            votes = votes & NAY;
            System.out.println("nodes were created without properties : NAY");
        }

        if (data.deletedRelationships().iterator().hasNext()) {

            Iterable<Relationship> rels = data.deletedRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) votes = votes & NAY;
                if (votes == NAY)
                    System.out.println("relationships were deleted but were attached to nodes with LocalTX Label : NAY");

            }
        }
        if (data.deletedNodes().iterator().hasNext()) {
            Iterable<Node> nodes = data.deletedNodes();
            for (Node node : nodes) {
                if (node.hasLabel(Label.label(LOCAL_TX))) votes = votes & NAY;
                if (votes == NAY) System.out.println("nodes were deleted but had LocalTx lable: NAY");
            }
        }

        if (data.createdRelationships().iterator().hasNext()) {
            Iterable<Relationship> rels = data.createdRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label(LOCAL_TX))) votes = votes & NAY;
                if (votes == NAY)
                    System.out.println("relationships were created but were attached to nodes with LocalTX Label : NAY");

            }
        }

        if (data.assignedRelationshipProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Relationship>> relPropEntry = data.assignedRelationshipProperties();

            for (PropertyEntry<Relationship> pe : relPropEntry) {

                if (pe.entity().getStartNode().hasLabel(Label.label(LOCAL_TX)) || !pe.entity().hasProperty(UUID)) {
                    votes = votes & NAY;
                    if (votes == NAY)
                        System.out.println("relationship properties were assigned to nodes with LocalTX label : NAY\n");
                    if (votes == NAY) System.out.println(" or uuid was assigned to relationship");
                }

            }

        }

        if (data.assignedNodeProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Node>> nodePropEntry = data.assignedNodeProperties();
            for (PropertyEntry<Node> ne : nodePropEntry) {
                if (!ne.entity().hasProperty(UUID)) {
                    votes = votes & NAY;
                    if (votes == NAY) System.out.println("no uuid was assigned to node");
                }
            }
        }

        return votes;
    }
}


