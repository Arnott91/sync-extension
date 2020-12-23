package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import java.util.Calendar;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static java.lang.String.format;

public class ReplicationEngine {
    private static final String PRUNE_QUERY = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated < %d DETACH DELETE tr " +
            "RETURN COUNT(tr) as deleted";
    private final Driver driver;
    private final ScheduledExecutorService execService;
    private ScheduledFuture<?> scheduledFuture;
    private GraphDatabaseService gds;
    private Log log;
    private long lastTransactionTimestamp;
    private long transactionRecordTimestamp;
    private final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated {id:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) " +
            "WHERE tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData";
    private final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated {id:'SINGLETON'}) " +
            "SET tr.lastTimeRecorded = %d";

    private static ReplicationEngine instance;
    private Status status;

    private ReplicationEngine(Driver driver) {
        this.driver = driver;
        this.execService = Executors.newScheduledThreadPool(1);
    }

    public synchronized static ReplicationEngine initialize(String remoteDatabaseURI, String username, String password) {
        if (instance != null) {
            instance.stop();
        }

        instance = new ReplicationEngine(
                GraphDatabase.driver(remoteDatabaseURI, AuthTokens.basic(username, password)));
        return instance();

    }

    public static ReplicationEngine instance() {
        return instance;
    }

    public synchronized void start() {
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            // first, grab the timestamp of the last transaction to be replicated locally.

            this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds);

            // next go and grab the transaction records from the remote database
            Result runReplication = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp));
            try {
                runReplication.forEachRemaining((a) -> {
                    try {
                        replicate(a);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                }
                );
            } catch (Exception e) {
                e.printStackTrace();
            }

            // I assume we can run to sessions back to back on the same thread in this scheduler.
            // Maybe we can provide more two threads?

            Result runPrune = driver.session().run(format(PRUNE_QUERY, getThreeDaysAgo()));
           // we can log the number of transactions pruned when we implement logging

        }, 0, 60L, TimeUnit.SECONDS);

        this.status = RUNNING;
    }

    private Consumer<? super Record> replicate(Record record) throws JSONException {

        // grab the transaction JSON data from the TransactionRecord node
        Value transactionData = record.get("data");
        // grab the timestamp from the TransactionRecord node
        Value transactionTime = record.get("time");

        GraphWriter graphWriter = new GraphWriter(transactionData.asString(), this.gds, log);
        graphWriter.executeCRUDOperation();
        TransactionHistoryManager.setLastReplicationTimestamp(gds,transactionTime.asLong());

        return null;
    }

    public void stop() {
        if (this.status == RUNNING) {
            scheduledFuture.cancel(true);
        }
        this.status = STOPPED;
    }

    public Status status() {
        return status;
    }

    public enum Status {RUNNING, STOPPED}

    private long daysAgo(int daysPast) {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -daysPast);
        return (cal.getTime()).getTime();
    }

    private long getThreeDaysAgo() {
        return daysAgo(3);
    }
}
