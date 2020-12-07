import org.apache.commons.collections.IteratorUtils;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class TransactionCrawler {



    public static void nodeCrawler (List<Node> nodes) {

        for (Node node : nodes) {

            System.out.println(format("A new node with ID %s should be created", node.getId()));
            labelCrawler(node);
            relationshipCrawler(node);
            propertyCrawler(node);
        }

    }

    public static void deletedNodeCrawler (List<Node> nodes) {

        for (Node node : nodes) {

            System.out.println(format("A node with ID %s should be deleted", node.getId()));

        }

    }
    private static void labelCrawler (Node node) {

        List<Label> labels = IteratorUtils.toList(node.getLabels().iterator());

        for (Label label : labels) {

            System.out.println(format("this node has the label %s", label.name()));

        }
    }
    private static void relationshipCrawler (Node node) {

        List<Relationship> rels = IteratorUtils.toList((node.getRelationships().iterator()));

        for (Relationship rel : rels) {

            System.out.println(format("this node has the relationship with type %s",rel.getType()));
            System.out.println(format("The relationship starts with node ID %s",rel.getStartNodeId()));
            System.out.println(format("The relationship ends with node ID %s",rel.getEndNodeId()));

        }
    }
    private static void propertyCrawler (Node node) {

        Map<String, Object> props = node.getAllProperties();

        for (Map.Entry<String, Object> entry : props.entrySet()) {
            System.out.println("node has property with name " + entry.getKey()
                    + ", and a value of " + entry.getValue());

        }
    }
}
