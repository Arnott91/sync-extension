package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.neo4j.configuration.BufferingLog;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static java.lang.String.format;

/**
 * Protocol is as follows.
 * <p>
 * The replication engine accepts a URI and authentication information and uses a scheduled executor service
 * to set up interval polling of a remote database for replicated transactions.  It also is responsible
 * for keeping track of the latest replicated transaction and for pruning replicated transactions at the source.
 * </p>
 *
 * @author Chris Upkes
 * @author Jim Webber
 */
@SuppressWarnings("DuplicatedCode")
public class ReplicationEngine {
    private static final String PRUNE_QUERY = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated < %d DETACH DELETE tr " +
            "RETURN COUNT(tr) as deleted";
    private static final String ST_DATA_JSON = "{\"statement\":\"true\"}";
    private static final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated {id:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private static final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData, tr.transactionStatement ORDER BY tr.timeCreated LIMIT %d";
    private static final String COUNT_QUERY = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated > %d RETURN count(tr) AS count";
    private static final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated {id:'SINGLETON'}) " +
            "SET tr.lastTimeRecorded = %d";
    private static final String ST_DATA_VALUE = "NO_STATEMENT";

    private final Driver driver;
    private final ScheduledExecutorService execService;

    private static ReplicationEngine instance;
    private static int runCount = 0;
    private ScheduledFuture<?> scheduledFuture;
    private GraphDatabaseService gds;
    private Log log;
    private long lastTransactionTimestamp;
    private long transactionRecordTimestamp;
    private Status status;
    private int records = 0;
    private int maxTxSize = 1000;
    private int pruningExpireDays = 3;
    private int syncIntervalInSeconds = 60;


    private ReplicationEngine(Driver driver) {
        this.driver = driver;
        this.execService = Executors.newScheduledThreadPool(1);
    }

    private ReplicationEngine(Driver driver, GraphDatabaseService gds) {
        this.driver = driver;
        this.execService = Executors.newScheduledThreadPool(1);
        this.gds = gds;
        this.log = ((GraphDatabaseAPI) gds).getDependencyResolver().resolveDependency( LogService.class ).getUserLog( getClass() );
        initConfig();
    }

    public static synchronized ReplicationEngine initialize(String remoteDatabaseURI, String username, String password, Set<String> hostNames) throws URISyntaxException {
        if (instance != null) {
            instance.stop();
        }

        instance = new ReplicationEngine(AddressResolver.createDriver(remoteDatabaseURI, username, password, hostNames));
        return instance();
    }

    public static synchronized ReplicationEngine initialize(String remoteDatabaseURI, String username, String password, GraphDatabaseService gds, Set<String> hostNames) throws URISyntaxException {
        if (instance != null) {
            instance.stop();
        }

        instance = new ReplicationEngine(
                AddressResolver.createDriver(remoteDatabaseURI, username, password, hostNames), gds);
        return instance();
    }

    public static synchronized ReplicationEngine initialize(String remoteDatabaseURI, String username, String password, GraphDatabaseService gds) {
        if (instance != null) {
            instance.stop();
        }

        instance = new ReplicationEngine(
                GraphDatabase.driver(remoteDatabaseURI, AuthTokens.basic(username, password)), gds);
        return instance();
    }

    public static ReplicationEngine instance() {
        return instance;
    }

    private void initConfig() {
        if (Configuration.isNotInitialized()) {
            Configuration.initializeFromNeoConf(log);
            Configuration.logSettings();
        }
        this.maxTxSize = Configuration.getMaxTxSize();
        this.pruningExpireDays = Configuration.getPruningExpireDays();
        this.syncIntervalInSeconds = Configuration.getSyncIntervalInSeconds();
    }

    public synchronized void start() {
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            try {
                TransactionFileLogger.appendPollingLog(String.format("Polling starting: %d", new Date(System.currentTimeMillis()).getTime()), log);
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("ReplicationEngine -> Grabbing the last timestamp");
            // find the last transaction replicated and get it's timestamp
            this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds);
            log.info("ReplicationEngine -> Grabbed the last timestamp");

            int count = countTransactionRecords();

            if (count > 0 && maxTxSize > 0) {
                startReplicationWithLimiter(count);
            }
            log.info("ReplicationEngine -> Starting pruning");

            long recordsPruned = pruneOldRecords();
            log.info("ReplicationEngine -> Pruning complete %d records pruned", recordsPruned);

            try {
                TransactionFileLogger.appendPollingLog(String.format("Polling stopping: %d", new Date(System.currentTimeMillis()).getTime()), log);
                TransactionFileLogger.appendPollingLog("Records written since engine start: " + records, log);
                TransactionFileLogger.appendPollingLog("Records pruned: " + recordsPruned, log);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }, 0, syncIntervalInSeconds, TimeUnit.SECONDS);


        this.status = RUNNING;
    }

    private int countTransactionRecords() {
        try{
            Result result = driver.session().run(format(COUNT_QUERY, lastTransactionTimestamp));

            if (result.hasNext()) {
                return result.next().get("count").asInt();
            }
        } catch (Neo4jException e) {
            log.warn("Could not create driver session. Reason: %s", e.getMessage());
        }
        return 0;
    }

    private void startReplicationWithLimiter(int count) {
        int recordsRemaining = count;
        log.info("Beginning replication with %d records to replicate, and a limiter of %d", recordsRemaining, maxTxSize);

        try {
            while (recordsRemaining > 0) {
                // pull all TransactionRecord nodes newer than last replicated.
                Result runReplication = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp, maxTxSize));

                if (runReplication.keys() != null && runReplication.hasNext()) {
                    runReplication.forEachRemaining(a -> {
                                try {
                                    replicate(a);
                                    TransactionFileLogger.appendPollingLog(String.format("Polling source: %d", new Date(System.currentTimeMillis()).getTime()), log);
                                } catch (JSONException | IOException e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                    );
                }
                recordsRemaining -= maxTxSize;
            }
            log.info("Replication complete");
        } catch (Neo4jException e) {
            log.warn("Could not create driver session. Reason: %s", e.getMessage());
        }
    }

    private long pruneOldRecords() {
        try(org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
            org.neo4j.graphdb.Result pruneResult = tx.execute(format(PRUNE_QUERY, daysAgo(pruningExpireDays)));

            if (pruneResult.hasNext()) {
                tx.commit();
                return (long) pruneResult.next().get("deleted");
            }
            return 0;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return 0;
    }

    public synchronized void testPolling(int polls) throws InterruptedException {

        Log bufferingLog = new BufferingLog();

        Runnable replicationRoutine = () -> {

            runCount++;
            log.info("Im running a task!");
            try {
                TransactionFileLogger.appendPollingLog(String.format("Polling starting: %d", new Date(System.currentTimeMillis()).getTime()), bufferingLog);
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("Grabbing the last timestamp");
            this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds);
            log.info("Grabbed the last timestamp");

            Result runReplication = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp));


            if (runReplication.keys() != null && runReplication.hasNext()) {
                try {
                    runReplication.forEachRemaining(a -> {
                                try {
                                    replicate(a);
                                    TransactionFileLogger.appendPollingLog(String.format("Polling source: %d", new Date(System.currentTimeMillis()).getTime()), bufferingLog);


                                } catch (JSONException | IOException e) {
                                    e.printStackTrace();
                                }
                            }
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            log.info("Starting pruning");

            long recordsPruned = pruneOldRecords();
            log.info("Pruning complete %d records pruned%n", recordsPruned);

            try {
                TransactionFileLogger.appendPollingLog(String.format("Polling stopping: %d", new Date(System.currentTimeMillis()).getTime()), bufferingLog);
                TransactionFileLogger.appendPollingLog("Records written since engine start: " + records, bufferingLog);
                TransactionFileLogger.appendPollingLog("Records pruned: " + recordsPruned, bufferingLog);
            } catch (IOException e) {
                e.printStackTrace();
            }

        };
        ScheduledFuture<?> scheduler = execService.scheduleAtFixedRate(replicationRoutine, 5, 60, TimeUnit.SECONDS);

        while (true) {
            log.info("run count :" + runCount);
            scheduler.wait(10000);
            if (runCount == polls) {
                log.info("Count is %d, cancel the scheduledFuture!%n", polls);
                scheduler.cancel(true);
                execService.shutdown();
                break;
            }
        }
    }

    private void replicate(Record record) throws JSONException {

        // grab the timestamp from the TransactionRecord node
        Value transactionTime = record.get("tr.timeCreated");
        if (!record.get("tr.transactionData").asString().equals(ST_DATA_JSON)) {

            // grab the transaction JSON data from the TransactionRecord node

            String transactionData = record.get("tr.transactionData").asString();


            try (org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
                TransactionDataHandler txHandler = new TransactionDataHandler(transactionData, tx, log);
                txHandler.executeCRUDOperation();
                tx.commit();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                log.debug("ReplicationEngine -> Completed replication tx");
            }


        } else if (!record.get("tr.transactionStatement").asString().equals(ST_DATA_VALUE)) {

            try (org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
                tx.execute(record.get("tr.transactionStatement").asString());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                log.debug("ReplicationEngine -> Completed replication tx");
            }

        }
        TransactionHistoryManager.setLastReplicationTimestamp(gds, transactionTime.asLong());
        this.records++;
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

    private long daysAgo(int daysPast) {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -daysPast);
        return (cal.getTime()).getTime();
    }

    public enum Status {RUNNING, STOPPED}
}
