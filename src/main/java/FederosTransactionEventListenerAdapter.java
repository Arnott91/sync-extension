import org.neo4j.graphdb.*;
import org.apache.commons.collections.IteratorUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.logging.Log;


import java.util.List;

import static java.lang.String.format;

// Definitely shouldn't be a string in the long run...
public class FederosTransactionEventListenerAdapter extends TransactionEventListenerAdapter<String> {

    public static final Label                      EVENT_NODE_LABEL                = Label
            .label("Event");
    public static final Label                      REP_NODE_LABEL                = Label
            .label("ReplicationDB");
    private static final String                    DONT_REPLICATE                  = "dont_replicate";
    public static final Label                      NO_AUDIT_NODE_LABEL             = Label
            .label(DONT_REPLICATE);
    private static final String                    UUID                            = "uuid";
    private static Log                             log;
    private static boolean                         enabled                         = true;
    private static boolean                         replicationEnabled              = true;
    private static boolean                         eventEnabled                    = true;
    private TransactionRecord                      transactionRecord;

    /*@Override
    TO_DO :  Override the beforeCommit method
    initialize a TransactionRecorder with a TransactionData object or change the recorder class to be static
    then get a TransactionRecord.  Use encapsulation and leave the JSON translation to the transaction record object
    then get an instance of a writer (still unsure if we need an instance or just a static class), however,
    get a writer (first a file writer) and then write the transaction record JSON to a file
    */

    @Override
    public void afterCommit(TransactionData data, String state, GraphDatabaseService databaseService) {
        super.afterCommit(data, state, databaseService);
        //TransactionRecorder myTransactionRecorder = new TransactionRecorder(data, state, databaService)

        transactionRecord = new TransactionRecord();
        Iterable<Node> newNodes = data.createdNodes();
        List<Node> newNodeList = IteratorUtils.toList(newNodes.iterator());
        System.out.println(format("A new node with ID %s was created", newNodeList.get(0).getId()));
        // inspect 'data' here and send it to the "integration DB" (using the Java driver on another thread perhaps?)
        System.out.println(format("Created %d nodes in transaction", IteratorUtils.toArray(newNodes.iterator()).length));
    }
}

