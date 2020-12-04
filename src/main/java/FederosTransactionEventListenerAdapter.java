import org.apache.commons.collections.IteratorUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;

import static java.lang.String.format;

// Definitely shouldn't be a string in the long run...
public class FederosTransactionEventListenerAdapter extends TransactionEventListenerAdapter<String> {
    @Override
    public void afterCommit(TransactionData data, String state, GraphDatabaseService databaseService) {
        super.afterCommit(data, state, databaseService);
               
        // inspect 'data' here and send it to the "integration DB" (using the Java driver on another thread perhaps?)
        System.out.println(format("Created %d nodes in transaction", IteratorUtils.toArray(data.createdNodes().iterator()).length));
    }
}
