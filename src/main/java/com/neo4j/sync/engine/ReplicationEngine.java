package com.neo4j.sync.engine;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static java.lang.String.format;

public class ReplicationEngine {
    private final Driver driver;
    private final ScheduledExecutorService execService;
    private ScheduledFuture<?> scheduledFuture;
    private Status status = STOPPED;
    private long lastTransactionTimestamp;
    private long transactionRecordTimestamp;
    private final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated {id:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) " +
            "WHERE tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData";
    private final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated {id:'SINGLETON'}) " +
            "SET tr.lastTimeRecorded = %d";

    public ReplicationEngine(Driver driver) {
        this(driver, Executors.newScheduledThreadPool(1));
    }

    ReplicationEngine(Driver driver, ScheduledExecutorService executorService) {
        this.driver = driver;
        this.execService = executorService;
    }

    public synchronized void start() {
        if (status == RUNNING) {
            return;
        }
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            // first, grab the timestamp of the last transaction to be replicated locally.
            // tx.findNode(Label.label("LastTransactionReplicated"))
            // and get the lastTimeRecorded property
            // or run the LOCAL_TIMESTAMP_QUERY;

            // next go and grab the transaction records from the remote database
            Result run = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp));

            run.forEachRemaining(System.out::println);

            // next, write transactionData to the local database using the GraphWriter
            // finally update the local singleton node that keeps track of the timestamp of the last replicated record
            // tx.execute(String.format(UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY, this.transactionRecordTimestamp)

        }, 0, 60L, TimeUnit.SECONDS);
        status = RUNNING;
    }

    public void stop() {
        if (status == STOPPED) {
            return;
        }
        scheduledFuture.cancel(true);
        status = Status.STOPPED;
    }

    public Status status() {
        return status;
    }

    public enum Status {RUNNING, STOPPED}
}
