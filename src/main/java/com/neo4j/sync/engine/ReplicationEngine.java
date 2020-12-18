package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
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
    private GraphDatabaseService gds;
    private Log log;
    private Status status = STOPPED;
    private long lastTransactionTimestamp;
    private long transactionRecordTimestamp;
    private final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated {id:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) " +
            "WHERE tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData";
    private final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated {id:'SINGLETON'}) " +
            "SET tr.lastTimeRecorded = %d";

    public ReplicationEngine(Driver driver, GraphDatabaseService gds, Log log) {
        this(driver, Executors.newScheduledThreadPool(1));
        this.gds = gds;
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

                    this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds);

                    // next go and grab the transaction records from the remote database
                    Result run = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp));

                    run.forEachRemaining(System.out::println);

                    // this.transactionRecordTimestamp = run.next().get("timeCreated").asLong();

                    // next, write transactionData to the local database using the GraphWriter
                    //
//            try  {
//                GraphWriter writer = new GraphWriter("{test:}",gds, log);
//                writer.executeCRUDOperation();
//
//
//            } catch (JSONException e){
//
//                log.error(e.getMessage());
//
//            } finally
//            {
                    //TransactionHistoryManager.setLastReplicationTimestamp(gds,this.transactionRecordTimestamp);
     //           }


//            ;


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
