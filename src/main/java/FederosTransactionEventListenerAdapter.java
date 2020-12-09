import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import java.util.function.Supplier;


/**
 * Protocol is as follows.
 *
 * 1. All changes are copied into the integration db (or in a linked list) beforeCommit
 * 2a. afterCommit the changes are made visible (e.g. connect this linked list into the main linked list)
 * 2b. afterRollback delete this linked list
 */

public class FederosTransactionEventListenerAdapter implements TransactionEventListener<Node> {

    private String beforeCommitTxId;
    private final String TX_RECORD_LABEL = "TransactionRecord";
    private final String TX_RECORD_NODE_PRIMARY_KEY = "transactionId";
    private final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final String TX_RECORD_STATUS_KEY = "status";
    private final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private final String AFTER_COMMIT_NODE_TIME_KEY = "commitTime";
    private long node_id;
    private boolean replicate;

    @Override
    public Node beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
            throws Exception {


        TransactionRecorder txRecorder = new TransactionRecorder(data);
        TransactionRecord txRecord = txRecorder.serializeTransaction();
        if (txRecord == null) {
            this.replicate = false;
        } else
        {
            this.replicate = true;
        }

        if (replicate) {

            this.beforeCommitTxId = txRecord.getTransactionUUID();

            try (Transaction tx = databaseService.beginTx()) {
                Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
                txRecordNode.setProperty(TX_RECORD_STATUS_KEY, txRecord.getStatus());
                txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, txRecord.getTimestampCreated());
                txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, txRecord.getTransactionUUID());
                txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, txRecord.getTransactionData());
                tx.commit();





            } catch (Exception e) {
                // log exception
                //this.logException(e, databaseService);
                System.out.println(e.getMessage());

            }
        }
//       if (txRecord != null) {
//           this.beforeCommitTxId = txRecord.getTransactionUUID();
//           String myQuery = "MERGE (tr:TransactionRecord {uuid:1})";
//           try {
//               databaseService.executeTransactionally(myQuery);
//
//           } catch (Exception e) {
//               // log exception
//               this.logException(e, databaseService);
//
//           }
//       } return txRecordNode;

        return null;
    }

    @Override
    public void afterCommit(TransactionData data, Node startNode, GraphDatabaseService databaseService) {
        // make our linked list of changes permanent, including the tx ID which comes from data.getTransactionId()
        // make sure that startNode is the node we created in beforeCommit.
        // if so, then update it in the database.
        // for now it is one node per transaction

        if (replicate) {
            try (Transaction tx = databaseService.beginTx()) {
                Node txRecordNode = tx.findNode(Label.label(TX_RECORD_LABEL), TX_RECORD_NODE_BEFORE_COMMIT_KEY, this.beforeCommitTxId);
                txRecordNode.setProperty(TX_RECORD_NODE_PRIMARY_KEY, data.getTransactionId());
                txRecordNode.setProperty(AFTER_COMMIT_NODE_TIME_KEY, data.getCommitTime());
                tx.commit();


            } catch (Exception e) {
                // log exception
                //this.logException(e, databaseService);
                System.out.println(e.getMessage());

            }
        }
//        String updateQuery = "MATCH (tr:TransactionRecord) "
//                + "WHERE tr."
//                + this.TX_RECORD_NODE_BEFORE_COMMIT_KEY
//                + "='"
//                + this.beforeCommitTxId
//                + "' "
//                + "SET tr.transactionId = '"
//                + data.getTransactionId()
//                + "'";
//
//            try  {
//                databaseService.executeTransactionally(updateQuery);
//
//            } catch (Exception e)
//            {
//                // log exception
//                this.logException(e, databaseService);
//
//            }
    }

    @Override
    public void afterRollback(TransactionData data, Node startNode, GraphDatabaseService databaseService) {
        // delete our linked list of changes.
        // for now it is one node per transaction

        if (replicate) {
            try (Transaction tx = databaseService.beginTx()) {
                Node txRecordNode = tx.findNode(Label.label(TX_RECORD_LABEL), TX_RECORD_NODE_BEFORE_COMMIT_KEY, this.beforeCommitTxId);
                txRecordNode.delete();
                tx.commit();


            } catch (Exception e) {
                // log exception
                //this.logException(e, databaseService);
                System.out.println(e.getMessage());

            }
        }
//        String deleteQuery = "MATCH (tr:TransactionRecord) "
//                + "WHERE tr."
//                + this.TX_RECORD_NODE_BEFORE_COMMIT_KEY
//                + "='"
//                + this.beforeCommitTxId
//                + "' "
//                + "DETACH DELETE tr";
//
//        try  {
//            databaseService.executeTransactionally(deleteQuery);
//
//        } catch (Exception e)
//        {
//            // log exception
//            this.logException(e, databaseService);
//
//        }
    }


    private void logException(Exception e, GraphDatabaseService databaseService) {

        Supplier<LogProvider> logProviderSupplier = ((GraphDatabaseAPI) databaseService).getDependencyResolver().provideDependency(LogProvider.class);
        LogProvider logProvider = logProviderSupplier.get();
        Log log = logProvider.getLog(this.getClass());
        log.info("Federos Service Extension Event Handler error: " + e.getMessage());

    }
}

