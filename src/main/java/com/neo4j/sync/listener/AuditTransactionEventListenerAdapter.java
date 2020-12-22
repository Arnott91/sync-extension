package com.neo4j.sync.listener;

//import com.neo4j.sync.start.Startup;

import com.neo4j.sync.engine.TransactionFileLogger;
import com.neo4j.sync.engine.TransactionRecord;
import com.neo4j.sync.engine.TransactionRecorder;
import com.neo4j.sync.start.SyncExtensionFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.neo4j.driver.GraphDatabase.driver;

/**
 * Protocol is as follows.
 * <p>
 * The listener adapter is intended to capture client transactions and record tx history for replication.
 * Change capture happens before commit and logging occurs after.
 * Transaction records are written locally and pulled by the destination server.
 * Clients can also read a copy of the transaction history from the transaction replication log files.
 * </p>
 *
 * @author Chris Upkes
 * @author Jim Webber
 */
public class AuditTransactionEventListenerAdapter implements TransactionEventListener<Node> {

    public static final String INTEGRATION_DATABASE = "INTEGRATION.DATABASE";

    private final String TX_RECORD_LABEL = "TransactionRecord";
    private final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final String TX_RECORD_STATUS_KEY = "status";
    private final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";

    private String beforeCommitTxId;
    private long transactionTimestamp;
    private String txData;
    private boolean logTransaction = false;
    private boolean replicate = false;
    private TransactionRecord txRecord;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private Node commitNode;

    @Override
    public Node beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService sourceDatabase)
            throws Exception {



        // ge a handle to the transaction recorder.  This will grab information from the Transaction Data object
        // and populate a transaction record that we can use to both write a TransactionRecord node to the
        // local database and also to log the transaction in any of the logs.
        System.out.println("In the beforeCommit method of our event listener");

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


            try (Transaction tx = sourceDatabase.beginTx()) {
                Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
                txRecordNode.setProperty(TX_RECORD_STATUS_KEY, txRecord.getStatus());
                txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, txRecord.getTimestampCreated());
                txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, txRecord.getTransactionUUID());
                txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, txRecord.getTransactionData());
                tx.commit();
            } catch (Exception e) {
                //getLog(sourceDatabase).error(e.getMessage(), e);
                System.out.println(e.getMessage());
            } finally {
                logTransaction = true;
            }

        }

        // I tried returning the txRecordNode in the above try - catch statement and I didn't get a handle
        // to it in the afterCommit.  Should double-check.
        return null;
    }

    private Log getLog(GraphDatabaseService databaseService) {
        return ((GraphDatabaseAPI) databaseService).getDependencyResolver().provideDependency(LogProvider.class).get().getLog(this.getClass());
    }

    @Override
    public void afterCommit(TransactionData data, Node startNode, GraphDatabaseService sourceDatabase) {
        // log our committed transactions to the transaction log.
        // we can then compare any written nodes in the transaction log that also exist in the rollback logs.


        if (logTransaction) {
            try {
                TransactionFileLogger.AppendTransactionLog(txData, beforeCommitTxId, data.getTransactionId(),
                        transactionTimestamp,
                        getLog(sourceDatabase));
            } catch (Exception e) {
                //getLog(sourceDatabase).error(e.getMessage(), e);
                System.out.println(e.getMessage());
            } finally {
                logTransaction = false;
            }
        }
        System.out.println("In the afterCommit method of our event listener");

    }

    @Override
    public void afterRollback(TransactionData data, Node startNode, GraphDatabaseService sourceDatabase) {
        // identify transactions that have rolled backed with their transaction UUID values so that we can
        // compare to the transaction log and look for written transaction records that were rolled back.

        System.out.println("In the afterRollback method of our event listener");

        if (replicate) {
            try {
                TransactionFileLogger.AppendRollbackTransactionLog(this.beforeCommitTxId, this.transactionTimestamp, getLog(sourceDatabase));
            } catch (Exception e) {
                // log exception
                //getLog(sourceDatabase).error(e.getMessage(), e);
                System.out.println(e.getMessage());
            }
        }
    }
}

class DatabaseWriterService implements Runnable {

    private final ExecutorService pool;
    private final TransactionRecord txRecord;

    public DatabaseWriterService(TransactionRecord txRecord, int poolSize)
            throws Exception {

        pool = Executors.newFixedThreadPool(poolSize);
        this.txRecord = txRecord;

    }

    public void run() { // run the service
        try {
            for (;;) {
                pool.execute(new Handler(txRecord));
            }
        } catch (Exception ex) {
            pool.shutdown();
        }
    }
}

class Handler implements Runnable {

    public static final String INTEGRATION_DATABASE = "INTEGRATION.DATABASE";

    private final String TX_RECORD_LABEL = "TransactionRecord";
    private final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final String TX_RECORD_STATUS_KEY = "status";
    private final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private TransactionRecord txRecord;
    private boolean logTransaction = false;



    Handler(TransactionRecord txRecord) { this.txRecord = txRecord; }
    public void run() {
        GraphDatabaseService destinationDatabase = SyncExtensionFactory.CustomGraphDatabaseLifecycle.getDatabase(INTEGRATION_DATABASE);
        try (Transaction tx = destinationDatabase.beginTx()) {
            Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
            txRecordNode.setProperty(TX_RECORD_STATUS_KEY, txRecord.getStatus());
            txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, txRecord.getTimestampCreated());
            txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, txRecord.getTransactionUUID());
            txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, txRecord.getTransactionData());
            tx.commit();
        } catch (Exception e) {
            //getLog(sourceDatabase).error(e.getMessage(), e);
            System.out.println(e.getMessage());
        } finally {
            logTransaction = true;
        }
        // read and service request on socket
    }
}

