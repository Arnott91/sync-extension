import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;


/**
 * Protocol is as follows.
 *
 * 1. All changes are copied into the integration db (or in a linked list) beforeCommit
 * 2a. afterCommit the changes are made visble (e.g. connect this linked list into the main linked list)
 * 2b. afterRollback delete this linked list
 */

public class FederosTransactionEventListenerAdapter implements TransactionEventListener<Node> {

//    private TransactionRecord transactionRecord;
//    List <Node> newNodeList;
//    List <Node> deletedNodeList;


    @Override
    public Node beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
            throws Exception {

        /*newNodeList = IteratorUtils.toList(data.createdNodes().iterator());
        deletedNodeList = IteratorUtils.toList(data.deletedNodes().iterator());



        if (!deletedNodeList.isEmpty()) TransactionCrawler.deletedNodeCrawler(deletedNodeList);
        if (!newNodeList.isEmpty()) TransactionCrawler.nodeCrawler(newNodeList);*/

        TransactionRecorder txRecorder = new TransactionRecorder(data);
        txRecorder.serializeTransaction();



        return null;

    }

    @Override
    public void afterCommit(TransactionData data, Node startNode, GraphDatabaseService databaseService) {
        // make our linked list of changes permanent
    }

    @Override
    public void afterRollback(TransactionData transactionData, Node startNode, GraphDatabaseService graphDatabaseService) {
        // delete our linked list of changes.
    }


}

