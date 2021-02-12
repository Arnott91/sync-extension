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
    private static final String PRUNE_QUERY = "MATCH (tr:TransactionRecord) WHERE NOT tr:StatementRecord AND tr.timeCreated < %d DETACH DELETE tr " +
            "RETURN COUNT(tr) as deleted";
    private static final String PRUNE_STMT_QUERY = "MATCH (tr:StatementRecord) WHERE tr.timeCreated < %d DETACH DELETE tr " +
            "RETURN COUNT(tr) as deleted";
    private static final String ST_DATA_JSON = "{\"statement\":\"true\"}";
    private static final String LOCAL_TIMESTAMP_QUERY = "MATCH (ltr:LastTransactionReplicated {id:'SINGLETON'}) RETURN ltr.lastTimeRecorded";
    private static final String REPLICATION_QUERY = "MATCH (tr:TransactionRecord) WHERE NOT tr:StatementRecord AND tr.timeCreated > %d " +
            "RETURN tr.uuid, tr.timeCreated, tr.transactionData, tr.transactionStatement ORDER BY tr.timeCreated LIMIT %d";
    private static final String STMT_REPL_QUERY = "MATCH (tr:StatementRecord) WHERE tr.timeCreated > %d RETURN tr.uuid, " +
            "tr.timeCreated, tr.transactionData, tr.transactionStatement ORDER BY tr.timeCreated LIMIT %d;";
    private static final String COUNT_QUERY = "MATCH (tr:TransactionRecord) WHERE NOT tr:StatementRecord AND tr.timeCreated > %d RETURN count(tr) AS count";
    private static final String COUNT_STMT_QUERY = "MATCH (tr:StatementRecord) WHERE tr.timeCreated > %d RETURN count(tr) AS count";
    private static final String UPDATE_LAST_TRANSACTION_TIMESTAMP_QUERY = "MERGE (ltr:LastTransactionReplicated {id:'SINGLETON'}) " +
            "SET tr.lastTimeRecorded = %d";
    private static final String ST_DATA_VALUE = "NO_STATEMENT";

    private final Driver driver;
    private final ScheduledExecutorService execService;

    private static ReplicationEngine instance;
    private static ReplicationEngine stmtReplInstance;
    private static int runCount = 0;
    private ScheduledFuture<?> scheduledFuture;
    private ScheduledFuture<?> stmtScheduledFuture;
    private GraphDatabaseService gds;
    private Log log;
    private long lastTransactionTimestamp;
    private long lastStmtTxTimestamp;
    private Status status = STOPPED;
    private Status stmtReplStatus = STOPPED;
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

    public static synchronized ReplicationEngine initializeStmtRepl(String remoteDatabaseURI, String username, String password, GraphDatabaseService gds) {
        if (stmtReplInstance != null) {
            stmtReplInstance.stop();
        }

        stmtReplInstance = new ReplicationEngine(
                GraphDatabase.driver(remoteDatabaseURI, AuthTokens.basic(username, password)), gds);
        return stmtReplInstance;
    }

    public static ReplicationEngine instance() {
        return instance;
    }
    public static ReplicationEngine stmtReplInstance() {
        return stmtReplInstance;
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
                TransactionFileLogger.appendPollingLog(String.format("[Graph repl] Polling starting: %d", new Date(System.currentTimeMillis()).getTime()), log);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            log.info("ReplicationEngine -> [Graph repl] Grabbing the last timestamp");
            // find the last transaction replicated and get it's timestamp
            this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds, TransactionType.GRAPH);
            log.info("ReplicationEngine -> [Graph repl] Grabbed the last timestamp");

            int count = countTransactionRecords(COUNT_QUERY, lastTransactionTimestamp);

            if (count > 0 && maxTxSize > 0) {
                startReplicationWithLimiter(count);
            }
            log.info("ReplicationEngine -> [Graph repl] Starting pruning");

            long recordsPruned = pruneOldRecords(PRUNE_QUERY);
            log.info("ReplicationEngine -> [Graph repl] Pruning complete %d records pruned", recordsPruned);

            try {
                TransactionFileLogger.appendPollingLog(String.format("[Graph repl] Polling stopping: %d", new Date(System.currentTimeMillis()).getTime()), log);
                TransactionFileLogger.appendPollingLog("[Graph repl] Records written since engine start: " + records, log);
                TransactionFileLogger.appendPollingLog("[Graph repl] Records pruned: " + recordsPruned, log);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

        }, 0, syncIntervalInSeconds, TimeUnit.SECONDS);

        this.status = RUNNING;
    }

    public synchronized void startStmtRepl() {
        stmtScheduledFuture = execService.scheduleAtFixedRate(() -> {
            try {
                TransactionFileLogger.appendPollingLog(String.format("[Statement repl] Polling starting: %d", new Date(System.currentTimeMillis()).getTime()), log);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            log.info("ReplicationEngine -> [Statement repl] Grabbing the last timestamp");
            // find the last transaction replicated and get it's timestamp
            this.lastStmtTxTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds, TransactionType.SCHEMA);
            log.info("ReplicationEngine -> [Statement repl] Grabbed the last timestamp");

            int count = countTransactionRecords(COUNT_STMT_QUERY, lastStmtTxTimestamp);

            if (count > 0 && maxTxSize > 0) {
                startStmtReplicationWithLimiter(count);
            }
            log.info("ReplicationEngine -> [Statement repl] Starting pruning");

            long recordsPruned = pruneOldRecords(PRUNE_STMT_QUERY);
            log.info("ReplicationEngine -> [Statement repl] Pruning complete %d records pruned", recordsPruned);

            try {
                TransactionFileLogger.appendPollingLog(String.format("[Statement repl] Polling stopping: %d", new Date(System.currentTimeMillis()).getTime()), log);
                TransactionFileLogger.appendPollingLog("[Statement repl] Records written since engine start: " + records, log);
                TransactionFileLogger.appendPollingLog("[Statement repl] Records pruned: " + recordsPruned, log);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }

        }, 0, syncIntervalInSeconds, TimeUnit.SECONDS);

        this.stmtReplStatus = RUNNING;
    }

    private int countTransactionRecords(String query, long timestamp) {
        try{
            Result result = driver.session().run(format(query, timestamp));

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
        log.info("Beginning graph replication with %d records to replicate, and a limiter of %d", recordsRemaining, maxTxSize);

        try {
            while (recordsRemaining > 0) {
                // pull all TransactionRecord nodes newer than last replicated.
                Result runReplication = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp, maxTxSize));

                if (runReplication.keys() != null && runReplication.hasNext()) {
                    runReplication.forEachRemaining(a -> {
                                try {
                                    replicate(a, TransactionType.GRAPH);
                                    TransactionFileLogger.appendPollingLog(String.format("Polling source: %d", new Date(System.currentTimeMillis()).getTime()), log);
                                } catch (JSONException | IOException | Neo4jException e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                    );
                }
                recordsRemaining -= maxTxSize;
            }
            log.info("Graph replication complete");
        } catch (Neo4jException e) {
            log.warn("Could not create driver session. Reason: %s", e.getMessage());
        }
    }

    private void startStmtReplicationWithLimiter(int count) {
        int recordsRemaining = count;
        log.info("Beginning statement replication with %d records to replicate, and a limiter of %d", recordsRemaining, maxTxSize);

        try {
            while (recordsRemaining > 0) {
                // pull all TransactionRecord nodes newer than last replicated.
                Result runReplication = driver.session().run(format(STMT_REPL_QUERY, lastStmtTxTimestamp, maxTxSize));

                if (runReplication.keys() != null && runReplication.hasNext()) {
                    runReplication.forEachRemaining(a -> {
                                try {
                                    replicate(a, TransactionType.SCHEMA);
                                    TransactionFileLogger.appendPollingLog(String.format("Polling source: %d", new Date(System.currentTimeMillis()).getTime()), log);
                                } catch (JSONException | IOException | Neo4jException e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                    );
                }
                recordsRemaining -= maxTxSize;
            }
            log.info("Statement replication complete");
        } catch (Neo4jException e) {
            log.warn("Could not create driver session. Reason: %s", e.getMessage());
        }
    }

    private long pruneOldRecords(String query) {
        try(org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
            org.neo4j.graphdb.Result pruneResult = tx.execute(format(query, daysAgo(pruningExpireDays)));

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
            this.lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(gds, TransactionType.GRAPH);
            log.info("Grabbed the last timestamp");

            Result runReplication = driver.session().run(format(REPLICATION_QUERY, lastTransactionTimestamp));


            if (runReplication.keys() != null && runReplication.hasNext()) {
                try {
                    runReplication.forEachRemaining(a -> {
                                try {
                                    replicate(a, TransactionType.GRAPH);
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

            long recordsPruned = pruneOldRecords(PRUNE_QUERY);
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

    private void replicate(Record record, TransactionType transactionType) throws JSONException {

        // grab the timestamp from the TransactionRecord node
        Value transactionTime = record.get("tr.timeCreated");
        if (transactionType == TransactionType.GRAPH) {

            // grab the transaction JSON data from the TransactionRecord node
            String transactionData = record.get("tr.transactionData").asString();

            try (org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
                TransactionDataHandler txHandler = new TransactionDataHandler(transactionData, tx, log);
                txHandler.executeCRUDOperation();
                tx.commit();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                log.debug("ReplicationEngine -> Completed graph replication transaction");
            }
        } else {
            try (org.neo4j.graphdb.Transaction tx = gds.beginTx()) {
                tx.execute(record.get("tr.transactionStatement").asString());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                log.debug("ReplicationEngine -> Completed schema replication transaction");
            }
        }
        TransactionHistoryManager.setLastReplicationTimestamp(gds, transactionTime.asLong(), transactionType);
        this.records++;
    }

    public void stop() {
        if (this.status == RUNNING) {
            scheduledFuture.cancel(true);
        }
        this.status = STOPPED;
    }

    public void stopStmtRepl() {
        if (this.stmtReplStatus == RUNNING) {
            stmtScheduledFuture.cancel(true);
        }
        this.stmtReplStatus = STOPPED;
    }

    public Status status() {
        return status;
    }
    public Status stmtReplStatus() {
        return stmtReplStatus;
    }

    private long daysAgo(int daysPast) {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -daysPast);
        return (cal.getTime()).getTime();
    }

    public enum Status {RUNNING, STOPPED}
}
