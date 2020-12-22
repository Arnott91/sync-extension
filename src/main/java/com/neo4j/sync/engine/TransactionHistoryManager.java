package com.neo4j.sync.engine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import java.lang.String;

import java.util.Map;

public class TransactionHistoryManager {

    public static final String LAST_TIME_RECORDED = "lastTimeRecorded";
    private final static String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated:LocalTx {uuid:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private final static String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated:LocalTx {uuid:'SINGLETON'}) " +
            "SET ltr.lastTimeRecorded = toInteger(%d)";

    public static Long getLastReplicationTimestamp(GraphDatabaseService gds) {


        Long timeStamp =null;

        try (Transaction tx = gds.beginTx();
             Result result = tx.execute( LOCAL_TIMESTAMP_QUERY) )
        {
            while (result.hasNext()) {
                timeStamp = (long) result.next().get(LAST_TIME_RECORDED);
            }

            if (timeStamp == null) {
                tx.execute(String.format(UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY, 0L));
                timeStamp = 0L;

            }

            tx.commit();
        }


        return timeStamp;
    }

    public static void setLastReplicationTimestamp(GraphDatabaseService gds, long timeStamp) {

        try (Transaction tx = gds.beginTx()) {

            Result result = tx.execute(String.format(UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY, timeStamp));
            tx.commit();
        }


    }


}

