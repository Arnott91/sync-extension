package com.neo4j.sync.engine;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;

import java.sql.Timestamp;
import java.util.*;

public class ReplicationJudge {
    private static boolean NAY = false;
    private static boolean YEA = true;

    public static boolean approved(TransactionData data) {

        boolean votes = YEA;
        if (data.createdNodes().iterator().hasNext() && !data.assignedLabels().iterator().hasNext()) {
            votes = NAY;
        }
        if (data.assignedLabels().iterator().hasNext()) {

            Iterable<LabelEntry> labels = data.assignedLabels();
            for (LabelEntry le : labels)
                if (le.label().name().equals("LocalTx") || le.label().name().equals("DoNotReplicate"))
                    votes = votes & NAY;
        }

        if (data.createdNodes().iterator().hasNext() && !data.assignedNodeProperties().iterator().hasNext()) votes = votes & NAY;

        if (data.deletedRelationships().iterator().hasNext()) {

            Iterable<Relationship> rels = data.deletedRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label("LocalTx"))) votes = votes & NAY;

            }
        }
        if (data.deletedNodes().iterator().hasNext()) {
            Iterable<Node> nodes = data.deletedNodes();
            for (Node node : nodes) {
                if (node.hasLabel(Label.label("LocalTx"))) votes = votes & NAY;
            }
        }

        if (data.createdRelationships().iterator().hasNext()) {
            Iterable<Relationship> rels = data.createdRelationships();
            for (Relationship rel : rels) {
                if (rel.getStartNode().hasLabel(Label.label("LocalTx"))) votes = votes & NAY;

            }
        }

        if (data.assignedRelationshipProperties().iterator().hasNext()) {
            Iterable<PropertyEntry<Relationship>> relPropEntry = data.assignedRelationshipProperties();

            for (PropertyEntry<Relationship> pe : relPropEntry) {

                if (!pe.entity().getStartNode().hasLabel(Label.label("LocalTx"))) votes = votes & NAY;
                votes = pe.key().equals("uuid") ? votes & YEA : votes & NAY;
            }

        }

        if (data.assignedNodeProperties().iterator().hasNext())
        {
            Iterable<PropertyEntry<Node>> nodePropEntry = data.assignedNodeProperties();

            for (PropertyEntry<Node> ne : nodePropEntry) {
                votes = ne.key().equals("uuid") ? votes & YEA : votes & NAY;

            }
        }

        return votes;
    }
}


