package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * com.neo4j.sync.engine.Configuration class is designed to load dynamic replication configuration information
 * from a number of sources depending on the initialization method called.  Configuration can be read
 * from a JSON file in a filesystem or from the database associated with the CaptureTransactionEventListenerAdapter.
 *
 * @author Chris Upkes
 */

public class Configuration {

    public static final String REPLICATION_SETTINGS_LABEL = "ReplicationSettings";
    public static final String REPLICATION_SETTINGS_PK_VALUE = "singleton";
    public static final String BATCH_SIZE_KEY = "batchSize";
    public static final String OUT_BOUND_TX_DIR_KEY = "outBoundTxDir";
    public static final String OUT_BOUND_RB_DIR_KEY = "outBoundRBDir";
    public static final String IN_BOUND_TX_DIR_KEY = "inBoundTxDir";
    public static final String POLLING_DIR_KEY = "pollingDir";
    public static final String IN_BOUND_TX_LOG_FILE_KEY = "inBoundTxLogFile";
    public static final String OUT_BOUND_TX_LOG_FILE_KEY = "outBoundTxLogFile";
    public static final String IN_BOUND_RB_TX_FILE_KEY = "inBoundRbTxFile";
    public static final String POLLING_FILE_KEY = "pollingFile";
    public static final String CONFIGURATION_KEY = "configuration";
    public static final String REPLICATION_SETTINGS_PK = "id";
    // the existing value was used for my testing.
    // if you do read from a config, it will be a relative
    // path, pointing to the import directory
    private static final String CONFIG_FILE_PATH = "c:/CONFIG/";
    private static final String CONFIG_FILE_NAME = "replication.conf";
    // TODO:  if setAllParameters is tested, then don't initialize with defaults here.
    public static String TX_LOG_FILE_DIR = "c:/OUTBOUND_TX";
    public static String TX_RB_LOG_FILE_DIR = "c:/ROLLBACK_OUTBOUND_TX";
    public static String IB_TX_LOG_FILE_DIR = "c:/INBOUND_TX;";
    public static String POLL_LOG_FILE_DIR = "c:/POLLING";
    public static String TX_LOG_FILE_NAME_IN = "inbound_tx.log";
    public static String TX_LOG_FILE_NAME = "outbound_tx.log";
    public static String TX_RB_LOG_FILE_NAME = "rb_outbound_tx.log";
    public static String POLLING_LOG = "tx_poll.log";
    private static File configFile = null;
    private static JSONObject config = null;
    private static boolean initialized = false;
    private static int BATCH_SIZE = 100;
    private static Map<String, Object> logSettings;
    // field unused but the thinking is there may be a case where you want to
    // use only defaults and not load a file or read a db.  Maybe for testing.
    private final boolean useDefaults = true;

    public static Map<String, Object> getLogSettings() {
        return logSettings;
    }

    public static void InitializeFromNeoConf(Config neo4jConfig) {
        // TODO: initialize ReplicationSettings node in database
        if (isInitialized()) {
            setAllParameters(neo4jConfig);
        }

    }

    public static int getBatchSize() {
        return BATCH_SIZE;
    }

    public static int getBatchSize(boolean isTest) throws JSONException {
        if (isTest) {
            testInitializeJSON();
        } else {
            initializeJSON();
        }
        return BATCH_SIZE;
    }

    public static boolean isInitialized() {
        return initialized;
    }

    // *** UNTESTED ***
    public static void InitializeFromDB(GraphDatabaseService gdb) {
        // TODO: initialize ReplicationSettings node in database
        if (isInitialized()) {
            try (Transaction tx = gdb.beginTx()) {
                setAllParameters(tx.findNode(Label.label(REPLICATION_SETTINGS_LABEL), REPLICATION_SETTINGS_PK, REPLICATION_SETTINGS_PK_VALUE));
                initialized = true;
            }
        }
    }
    private static void initializeJSON() throws JSONException {
        // TODO: grab and read parameters from the neo4j.conf if possible
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        if (isInitialized()) {
            configFile = new File(CONFIG_FILE_PATH + CONFIG_FILE_NAME);
            config = new JSONObject(configFile.toString());
            BATCH_SIZE = Integer.parseInt(((JSONObject) (new JSONObject(configFile.toString())).get(CONFIGURATION_KEY)).get(BATCH_SIZE_KEY).toString());
        }
    }

    private static void testInitializeJSON() throws JSONException {
        // TODO: grab and read parameters from the neo4j.conf if possible
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        configFile = new File(CONFIG_FILE_NAME);
        config = new JSONObject(configFile.toString());
        BATCH_SIZE = Integer.parseInt(((JSONObject) (new JSONObject(configFile.toString())).get(CONFIGURATION_KEY)).get(BATCH_SIZE_KEY).toString());
    }

    private static void setAllParameters(Node parameters) {

        // **** UNTESTED ****
        BATCH_SIZE = Integer.parseInt(parameters.getProperty(BATCH_SIZE_KEY, 100).toString());
        logSettings = new HashMap<>();
        logSettings.put("TX_LOG_FILE_DIR", parameters.getProperty(OUT_BOUND_TX_DIR_KEY, TX_LOG_FILE_DIR));
        logSettings.put("TX_RB_LOG_FILE_DIR", parameters.getProperty(OUT_BOUND_RB_DIR_KEY, TX_RB_LOG_FILE_DIR));
        logSettings.put("IB_TX_LOG_FILE_DIR", parameters.getProperty(IN_BOUND_TX_DIR_KEY, IB_TX_LOG_FILE_DIR));
        logSettings.put("TX_LOG_FILE_NAME_IN", parameters.getProperty(IN_BOUND_TX_LOG_FILE_KEY, TX_LOG_FILE_NAME_IN));
        logSettings.put("POLL_LOG_FILE_DIR", parameters.getProperty(Configuration.POLLING_DIR_KEY, POLL_LOG_FILE_DIR));
        logSettings.put("TX_LOG_FILE_NAME", parameters.getProperty(OUT_BOUND_TX_LOG_FILE_KEY, TX_LOG_FILE_NAME));
        logSettings.put("TX_RB_LOG_FILE_NAME", parameters.getProperty(IN_BOUND_RB_TX_FILE_KEY, TX_RB_LOG_FILE_NAME));
        logSettings.put("POLLING_LOG", parameters.getProperty(POLLING_FILE_KEY, POLLING_LOG));

    }

    private static void setAllParameters(Config neo4jConfig) {

        // **** UNTESTED ****
        BATCH_SIZE = Integer.parseInt(neo4jConfig.getSetting("batch_size").toString());
        logSettings = new HashMap<>();
        logSettings.put("TX_LOG_FILE_DIR", neo4jConfig.getSetting(OUT_BOUND_TX_DIR_KEY));
        logSettings.put("TX_RB_LOG_FILE_DIR" ,neo4jConfig.getSetting(OUT_BOUND_RB_DIR_KEY));
        logSettings.put("IB_TX_LOG_FILE_DIR", neo4jConfig.getSetting(IN_BOUND_TX_DIR_KEY));
        logSettings.put("TX_LOG_FILE_NAME_IN", neo4jConfig.getSetting(IN_BOUND_TX_LOG_FILE_KEY));
        logSettings.put("POLL_LOG_FILE_DIR", neo4jConfig.getSetting(POLLING_DIR_KEY));
        logSettings.put("TX_LOG_FILE_NAME", neo4jConfig.getSetting(OUT_BOUND_TX_LOG_FILE_KEY));
        logSettings.put("TX_RB_LOG_FILE_NAME", neo4jConfig.getSetting(IN_BOUND_RB_TX_FILE_KEY));
        logSettings.put("POLLING_LOG", neo4jConfig.getSetting(POLLING_FILE_KEY));

    }
}


