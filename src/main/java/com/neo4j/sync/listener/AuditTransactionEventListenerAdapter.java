package com.neo4j.sync.listener;

import com.neo4j.sync.engine.TransactionFileLogger;
import com.neo4j.sync.engine.TransactionRecord;
import com.neo4j.sync.engine.TransactionRecorder;
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
 * The listener adapter is intended to capture client transactions and record tx history for replication.
 * Change capture happens before commit and logging occurs after.
 * Transaction records are written locally and pulled by the destination server.
 * Clients can also read a copy of the transaction history from the transaction replication log files.
 * @author Chris Upkes
 * @author Jim Webber
 *
 */

public class AuditTransactionEventListenerAdapter implements TransactionEventListener<Node> {

    private String beforeCommitTxId;
    private final String TX_RECORD_LABEL = "com.neo4j.sync.engine.TransactionRecord";
    private final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final String TX_RECORD_STATUS_KEY = "status";
    private final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private long transactionTimestamp;
    private String txData;
    private boolean logTransaction = false;
    private boolean replicate = false;

    @Override
    public Node beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
            throws Exception {


        System.out.println(" --> HERE!!!");

        // ge a handle to the transaction recorder.  This will grab information from the Transaction Data object
        // and populate a transaction record that we can use to both write a TransactionRecord node to the
        // local database and also to log the transaction in any of the logs.

        TransactionRecorder txRecorder = new TransactionRecorder(data);
        TransactionRecord txRecord = txRecorder.serializeTransaction();

        // Check to make sure that we the transaction listener wasn't invoked because we committed a
        // transactionRecord node to the database.  If the TransactionRecorder encounters
        // a node with the TransactionRecord label, it will just return null.

        this.replicate = txRecord != null;


        if (replicate) {

            // let's grab the uuid of the transaction and a timestamp for logging in afterCommit and rollback
            // methods.

            this.beforeCommitTxId = txRecord.getTransactionUUID();
            this.transactionTimestamp = System.currentTimeMillis();
            txData = txRecord.getTransactionData();

            // Populate the TransactionRecord node with required transaction replay and history
            // data and write locally.


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

            } finally
            {
                this.logTransaction = true;


            }
        }

        // I tried returning the txRecordNode in the above try - catch statement and I didn't get a handle
        // to it in the afterCommit.  Should double-check.
        return null;
//
    }

    @Override
    public void afterCommit(TransactionData data, Node startNode, GraphDatabaseService databaseService) {
        // log our committed transactions to the transaction log.
        // we can then compare any written nodes in the transaction log that also exist in the rollback logs.

        if (logTransaction) {
            try {

                TransactionFileLogger.AppendTransactionLog(this.txData ,this.beforeCommitTxId, data.getTransactionId(), this.transactionTimestamp);

            } catch (Exception e) {
                // log exception
                //this.logException(e, databaseService);
                System.out.println(e.getMessage());

            } finally
            {
                this.logTransaction = false;
            }

        }
    }

    @Override
    public void afterRollback(TransactionData data, Node startNode, GraphDatabaseService databaseService) {
        // identify transactions that have rolled backed with their transaction UUID values so that we can
        // compare to the transaction log and look for written transaction recrods that were rolled back.

        if (replicate) {
            try {
                TransactionFileLogger.AppendRollbackTransactionLog(this.beforeCommitTxId, this.transactionTimestamp);


            } catch (Exception e) {
                // log exception
                //this.logException(e, databaseService);
                System.out.println(e.getMessage());

            }
        }

    }


    private void logException(Exception e, GraphDatabaseService databaseService) {

        Supplier<LogProvider> logProviderSupplier = ((GraphDatabaseAPI) databaseService).getDependencyResolver().provideDependency(LogProvider.class);
        LogProvider logProvider = logProviderSupplier.get();
        Log log = logProvider.getLog(this.getClass());
        log.info("Federos Service Extension Event Handler error: " + e.getMessage());

    }
}

