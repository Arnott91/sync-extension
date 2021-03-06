package com.neo4j.sync.listener;


import com.neo4j.sync.engine.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;


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
public class CaptureTransactionEventListenerAdapter implements TransactionEventListener<Node> {
    public static final String LOCAL_TX = "LocalTx";

    //public static final String INTEGRATION_DATABASE = "INTEGRATION.DATABASE";

    private final String TX_RECORD_LABEL = "TransactionRecord";
    private final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final String TX_RECORD_STATUS_KEY = "status";
    private final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private final String ST_TX_RECORD_TX_DATA_KEY = "transactionStatement";
    private final String ST_DATA_VALUE = "NO_STATEMENT";

    private String beforeCommitTxId;
    private long transactionTimestamp;
    private String txData;
    private boolean logTransaction = false;
    private boolean replicate = true;
    private boolean justUpdatedTr = false;

    @Override
    public Node beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService sourceDatabase)
            throws Exception {

        // REGARDING DDL AND DCL: We can grab index info from the transaction object if we really want to go there.
        // Remember,  schema transactions cannot include DML transactions.
        // System.out.println(transaction.schema().getIndexes().iterator().next().getName());

        System.out.println("In the beforeCommit method of our event listener");

        if (ReplicationJudge.approved(data) && !this.justUpdatedTr) {
            // get a handle to the transaction recorder.  This will grab information from the Transaction Data object
            // and populate a transaction record that we can use to both write a TransactionRecord node to the
            // local database and also to log the transaction in any of the logs.
            TransactionRecorder txRecorder = new TransactionRecorder(data);
            TransactionRecord txRecord = txRecorder.serializeTransaction();
            // let's grab the uuid of the transaction and a timestamp for logging in afterCommit and rollback
            // methods.
            this.beforeCommitTxId = txRecord.getTransactionUUID();
            this.transactionTimestamp = System.currentTimeMillis();
            txData = txRecord.getTransactionData();

            // Populate the TransactionRecord node with required transaction replay and history
            // data and write locally.
            try (Transaction tx = sourceDatabase.beginTx()) {
                Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
                txRecordNode.addLabel(Label.label(LOCAL_TX));
                txRecordNode.setProperty(TX_RECORD_STATUS_KEY, txRecord.getStatus());
                txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, txRecord.getTimestampCreated());
                txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, txRecord.getTransactionUUID());
                txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, txRecord.getTransactionData());
                txRecordNode.setProperty(ST_TX_RECORD_TX_DATA_KEY, ST_DATA_VALUE);

                tx.commit();
            } catch (Exception e) {
                // TODO: figure out how to get a working handle to the internal logger.
                //getLog(sourceDatabase).error(e.getMessage(), e);
                e.printStackTrace();
            } finally {
                logTransaction = true;

            }

        } else {
            logTransaction = false;
        }
        this.justUpdatedTr = false;

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


        // TODO:  uncomment below and TEST
        // if (!Configuration.isInitialized()) Configuration.InitializeFromDB(sourceDatabase);
        // if (!TransactionFileLogger.isAreSettingsInitialized()) TransactionFileLogger.initSettings(Configuration.getLogSettings());
        // the transaction logging you see below has hard-coded file locations.
        if (logTransaction) {
            try {
                TransactionFileLogger.AppendTransactionLog(txData, beforeCommitTxId, data.getTransactionId(),
                        transactionTimestamp);
            } catch (Exception e) {
                //getLog(sourceDatabase).error(e.getMessage(), e);
                System.out.println(e.getMessage());
            } finally {
                logTransaction = false;
            }
            // TODO:  Figure out how to handle TransactionRecord nodes that don't have an InternalTransactionId
            try (Transaction tx = sourceDatabase.beginTx()) {
                Node txRecordNode = tx.findNode(Label.label(TX_RECORD_LABEL), TX_RECORD_NODE_BEFORE_COMMIT_KEY, beforeCommitTxId);
                txRecordNode.setProperty("internalTransactionId", data.getTransactionId());
                txRecordNode.setProperty("commitTime", data.getCommitTime());
                this.justUpdatedTr = true;
                tx.commit();
            } catch (Exception e) {
                //getLog(sourceDatabase).error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
        System.out.println("In the afterCommit method of our event listener");

    }

    @Override
    public void afterRollback(TransactionData data, Node startNode, GraphDatabaseService sourceDatabase) {
        // identify transactions that have rolled backed with their transaction UUID values so that we can
        // compare to the transaction log and look for written transaction records that were rolled back.
        // TODO: use the refactored file logger initialized from the database.
        System.out.println("In the afterRollback method of our event listener");

        if (replicate) {
            try {
                TransactionFileLogger.AppendRollbackTransactionLog(this.beforeCommitTxId, this.transactionTimestamp);
            } catch (Exception e) {
                // log exception
                //getLog(sourceDatabase).error(e.getMessage(), e);
                e.printStackTrace();
            }
        }
    }
}





