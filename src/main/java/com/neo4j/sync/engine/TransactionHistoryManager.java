package com.neo4j.sync.engine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

/***
 * The transaction history manager is used to keep track of the latest transaction replicated to a target database.
 * @author Chris Upkes
 */
public class TransactionHistoryManager {

    public static final String LAST_TIME_RECORDED = "ltr.lastTimeRecorded";
    private static final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated:LocalTx {uuid:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private static final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated:LocalTx {uuid:'SINGLETON'}) " +
            "SET ltr.lastTimeRecorded = toInteger(%d)";

    private TransactionHistoryManager() {
        // private constructor to hide implicit public one
    }

    public static Long getLastReplicationTimestamp(GraphDatabaseService gds) {
        Log log = ((GraphDatabaseAPI) gds).getDependencyResolver().resolveDependency( LogService.class )
                .getUserLog(TransactionHistoryManager.class);

        Long timeStamp =null;
        log.debug("TransactionHistoryManager -> Getting last replication timestamp");

        try (Transaction tx = gds.beginTx();
             Result result = tx.execute( LOCAL_TIMESTAMP_QUERY) ) {
            while (result.hasNext()) {
                timeStamp = (long) result.next().get(LAST_TIME_RECORDED);
            }

            if (timeStamp == null) {
                tx.execute(String.format(UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY, 0L));
                timeStamp = 0L;
            }

            tx.commit();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return timeStamp;
    }

    public static void setLastReplicationTimestamp(GraphDatabaseService gds, long timeStamp) {
        try (Transaction tx = gds.beginTx()) {
            tx.execute(String.format(UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY, timeStamp));
            tx.commit();
        }
    }

}

