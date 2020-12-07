import org.neo4j.cypher.internal.runtime.slotted.expressions.NodeProperty;
import org.neo4j.graphdb.*;
import org.apache.commons.collections.IteratorUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;


import java.util.List;
import java.util.Map;

import static java.lang.String.format;

// Definitely shouldn't be a string in the long run...
public class FederosTransactionEventListenerAdapter extends TransactionEventListenerAdapter<String> {

    private TransactionRecord transactionRecord;
    List <Node> newNodeList;
    List <Node> deletedNodeList;


    @Override
    public String beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
            throws Exception {

        newNodeList = IteratorUtils.toList(data.createdNodes().iterator());
        deletedNodeList = IteratorUtils.toList(data.deletedNodes().iterator());



        if (!deletedNodeList.isEmpty()) TransactionCrawler.deletedNodeCrawler(deletedNodeList);
        if (!newNodeList.isEmpty()) TransactionCrawler.nodeCrawler(newNodeList);



        return null;
    }

    @Override
    public void afterCommit(TransactionData data, String state, GraphDatabaseService databaseService) {
        super.afterCommit(data, state, databaseService);
        //TransactionRecorder myTransactionRecorder = new TransactionRecorder(data, state, databaService)


        // inspect 'data' here and send it to the "integration DB" (using the Java driver on another thread perhaps?)
        if (!newNodeList.isEmpty()) System.out.println(format("Created %d nodes in transaction", IteratorUtils.toArray(newNodeList.iterator()).length));
        if (!deletedNodeList.isEmpty()) System.out.println(format("Deleted %d nodes in transaction", IteratorUtils.toArray(deletedNodeList.iterator()).length));
    }

    private void nodePrinter (List<Node> nodes) {

        for (Node node : nodes) {

            System.out.println(format("A new node with ID %s was created", node.getId()));

        }

    }


}

