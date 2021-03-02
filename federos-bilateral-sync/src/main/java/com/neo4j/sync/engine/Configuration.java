package com.neo4j.sync.engine;

import org.neo4j.driver.net.ServerAddress;
import org.neo4j.logging.Log;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * com.neo4j.sync.engine.Configuration class is designed to load dynamic replication configuration information
 * from a number of sources depending on the initialization method called.  Configuration can be read
 * from a JSON file in a filesystem or from the database associated with the CaptureTransactionEventListenerAdapter.
 *
 * @author Chris Upkes
 */

public class Configuration {

    // the existing value was used for my testing.
    // if you do read from a config, it will be a relative
    // path, pointing to the import directory
    private static final String CONFIG_FILE_PATH = "/var/lib/neo4j/conf/neo4j.conf";
    private static boolean initialized = false;
    private static int BATCH_SIZE = 100;
    private static Properties properties;

    private static Log log;

    public static final String REPLICATION_SETTINGS_LABEL = "ReplicationSettings";
    public static final String REPLICATION_SETTINGS_PK_VALUE = "singleton";
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final String OUT_BOUND_TX_KEY = "vnd.assure1.sync.tx_out_log";
    public static final String TX_ROLLBACK_KEY = "vnd.assure1.sync.tx_rollback_log";
    public static final String IN_BOUND_TX_KEY = "vnd.assure1.sync.tx_in_log";
    public static final String POLLING_KEY = "vnd.assure1.sync.poller_log";
    public static final String LISTENER_DBS = "vnd.assure1.sync.listener_dbs";
    public static final String PRUNING_EXPIRE_DAYS_KEY = "vnd.assure1.sync.expire_days";
    public static final String MAX_TX_SIZE_KEY = "vnd.assure1.sync.max_tx_size";
    public static final String SYNC_INTERVAL_KEY = "vnd.assure1.sync.interval";
    public static final String REPLICATION_SETTINGS_PK = "id";

    private static final String DEFAULT_TX_IB_LOG_FILE = "/var/lib/neo4j/data/INBOUND_TX/inbound_tx.log";
    private static final String DEFAULT_TX_OUT_LOG_FILE = "/var/lib/neo4j/data/OUTBOUND_TX/outbound_tx.log";
    private static final String DEFAULT_TX_RB_LOG_FILE= "/var/lib/neo4j/data/ROLLBACK_OUTBOUND_TX/rb_outbound_tx.log";
    private static final String DEFAULT_POLLING_LOG = "/var/lib/neo4j/data/POLLING/tx_poll.log";
    private static final String DEFAULT_LISTENER_DB = "graph";
    private static final int DEFAULT_PRUNING_EXPIRE = 3;
    private static final int DEFAULT_MAX_TX_SIZE = 1000;
    private static final int DEFAULT_SYNC_INTERVAL_SEC = 30;

    private static String txInLogFile;
    private static String txOutLogFile;
    private static String txRbLogFile;
    private static String pollingLog;
    private static List<String> listenerDBs = new ArrayList<>();
    private static Integer pruningExpireDays;
    private static Integer maxTxSize;
    private static Integer syncIntervalInSeconds;

    private Configuration() {
        // private constructor to hide implicit public one
    }

    public static void initializeFromNeoConf(Log log) {
        Configuration.log = log;

        if (isNotInitialized()) {
            setNeo4jConfig();
        }
        setAllParameters(properties);
    }

    public static boolean isNotInitialized() {
        return !initialized;
    }

    public static void logSettings() {
        if (log.isDebugEnabled()) {
            log.debug("Federos Bilateral Sync - initializing with the following settings:");
            log.debug("%s = %s", OUT_BOUND_TX_KEY, txOutLogFile);
            log.debug("%s = %s", IN_BOUND_TX_KEY, txInLogFile);
            log.debug("%s = %s", TX_ROLLBACK_KEY, txRbLogFile);
            log.debug("%s = %s", POLLING_KEY, pollingLog);
            log.debug("%s = %d", PRUNING_EXPIRE_DAYS_KEY, pruningExpireDays);
            log.debug("%s = %d", MAX_TX_SIZE_KEY, maxTxSize);
            log.debug("%s = %d", SYNC_INTERVAL_KEY, syncIntervalInSeconds);
        }
    }

    private static void setNeo4jConfig() {
        try (InputStream inputStream = new FileInputStream(CONFIG_FILE_PATH)) {
            properties = new Properties();
            properties.load(inputStream);
            initialized = true;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private static void setAllParameters(Properties properties) {
        setTxInLogFile(properties.getProperty(IN_BOUND_TX_KEY));
        setTxOutLogFile(properties.getProperty(OUT_BOUND_TX_KEY));
        setTxRbLogFile(properties.getProperty(TX_ROLLBACK_KEY));
        setPollingLog(properties.getProperty(POLLING_KEY));
        setListenerDBs(properties.getProperty(LISTENER_DBS));
        setPruningExpireDays(Integer.parseInt(properties.getProperty(PRUNING_EXPIRE_DAYS_KEY)));
        setMaxTxSize(Integer.parseInt(properties.getProperty(MAX_TX_SIZE_KEY)));
        setSyncIntervalInSeconds(Integer.parseInt(properties.getProperty(SYNC_INTERVAL_KEY)));
    }

    //TODO: implement getServerAddresses method
    public static List<ServerAddress> getServerAddresses() {
        return null;
    }

    public static String getTxInLogFile() {
        return txInLogFile;
    }

    private static void setTxInLogFile(String txInLogFile) {
        if (null == txInLogFile) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", IN_BOUND_TX_KEY, DEFAULT_TX_IB_LOG_FILE);
            Configuration.txInLogFile = DEFAULT_TX_IB_LOG_FILE;
        } else {
            Configuration.txInLogFile = txInLogFile;
        }
    }

    public static String getTxOutLogFile() {
        return txOutLogFile;
    }

    private static void setTxOutLogFile(String txOutLogFile) {
        if (null == txOutLogFile) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", OUT_BOUND_TX_KEY, DEFAULT_TX_OUT_LOG_FILE);
            Configuration.txOutLogFile = DEFAULT_TX_OUT_LOG_FILE;
        } else {
            Configuration.txOutLogFile = txOutLogFile;
        }
    }

    public static String getTxRbLogFile() {
        return txRbLogFile;
    }

    private static void setTxRbLogFile(String txRbLogFile) {
        if (null == txRbLogFile) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", TX_ROLLBACK_KEY, DEFAULT_TX_RB_LOG_FILE);
            Configuration.txRbLogFile = DEFAULT_TX_RB_LOG_FILE;
        } else {
            Configuration.txRbLogFile = txRbLogFile;
        }
    }

    public static String getPollingLog() {
        return pollingLog;
    }

    private static void setPollingLog(String pollingLog) {
        if (null == pollingLog) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", POLLING_KEY, DEFAULT_POLLING_LOG);
            Configuration.pollingLog = DEFAULT_POLLING_LOG;
        } else {
            Configuration.pollingLog = pollingLog;
        }
    }

    public static List<String> getListenerDBs() {
        return listenerDBs;
    }

    public static void setListenerDBs(String listenerDBs) {
        if (null == listenerDBs) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", LISTENER_DBS, DEFAULT_LISTENER_DB);
            Configuration.listenerDBs = List.of(DEFAULT_LISTENER_DB);
        } else {
            Configuration.listenerDBs = Arrays.asList(listenerDBs.split(","));
        }
    }

    public static Integer getPruningExpireDays() {
        return pruningExpireDays;
    }

    private static void setPruningExpireDays(Integer pruningExpireDays) {
        if (null == pruningExpireDays) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", PRUNING_EXPIRE_DAYS_KEY, DEFAULT_PRUNING_EXPIRE);
            Configuration.pruningExpireDays = DEFAULT_PRUNING_EXPIRE;
        } else {
            Configuration.pruningExpireDays = pruningExpireDays;
        }
    }

    public static Integer getMaxTxSize() {
        return maxTxSize;
    }

    private static void setMaxTxSize(Integer maxTxSize) {
        if (null == maxTxSize) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", MAX_TX_SIZE_KEY, DEFAULT_MAX_TX_SIZE);
            Configuration.maxTxSize = DEFAULT_MAX_TX_SIZE;
        } else {
            Configuration.maxTxSize = maxTxSize;
        }
    }

    public static Integer getSyncIntervalInSeconds() {
        return syncIntervalInSeconds;
    }

    private static void setSyncIntervalInSeconds(Integer syncIntervalInSeconds) {
        if (null == syncIntervalInSeconds) {
            log.warn("Could not find '%s' config item; defaulting to '%s'", SYNC_INTERVAL_KEY, DEFAULT_SYNC_INTERVAL_SEC);
            Configuration.syncIntervalInSeconds = DEFAULT_SYNC_INTERVAL_SEC;
        } else {
            Configuration.syncIntervalInSeconds = syncIntervalInSeconds;
        }
    }
}


