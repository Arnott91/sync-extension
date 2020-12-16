package com.neo4j.sync.engine;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;

public class ReplicationEngine {
    private final Driver driver;
    private ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledFuture;
    private Status status;
    private long lastTransactionTimestamp;
    private final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated) RETURN ltr.lastTimeRecorded";
    private final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) " +
            "WHERE tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData";

    public ReplicationEngine(Driver driver ) {
        this.driver = driver;
    }

    public synchronized void start() {
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            // TODO: change this for real cypher that pulls from the remote database
            // first, grab the timestamp of the last transaction to be replicated locally.
            // tx.findNode(Label.label("LastTransactionReplicated"))
            // and get the lastTimeRecorded property
            // or run the LOCAL_TIMESTAMP_QUERY;

            Result run = driver.session().run(String.format(REPLICATION_QUERY, this.lastTransactionTimestamp));
            run.forEachRemaining(System.out::println);

        }, 0, 60L, TimeUnit.SECONDS);
        status = RUNNING;
    }

    public void stop() {
        scheduledFuture.cancel(true);
        status = Status.STOPPED;
    }

    public Status status() {
        return status;
    }

    public enum Status { RUNNING, STOPPED}
}
