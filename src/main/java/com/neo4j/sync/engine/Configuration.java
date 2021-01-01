package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.*;
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
    private static final String CONFIG_FILE_PATH = "c:/CONFIG/";

    private static final String CONFIG_FILE_NAME = "replication.conf";

    private static File configFile = null;

    private static JSONObject config = null;

    // field unused but the thinking is there may be a case where you want to
    // use only defaults and not load a file or read a db.  Maybe for testing.
    private boolean useDefaults = true;

    private static boolean initialized = false;

    private static int BATCH_SIZE = 100;

    // TODO:  if setAllParameters is tested, then don't initialize with defaults here.
    private static String TX_LOG_FILE_DIR = "c:/OUTBOUND_TX";
    private static String TX_RB_LOG_FILE_DIR = "c:/ROLLBACK_OUTBOUND_TX";
    private static String IB_TX_LOG_FILE_DIR = "c:/INBOUND_TX;";
    private static String POLL_LOG_FILE_DIR = "c:/POLLING";
    private static String TX_LOG_FILE_NAME_IN = "inbound_tx.log";
    private static String TX_LOG_FILE_NAME = "outbound_tx.log";
    private static String TX_RB_LOG_FILE_NAME = "rb_outbound_tx.log";
    private static String POLLING_LOG = "tx_poll.log";

    public static Map<String, Object> getLogSettings() {
        return logSettings;
    }

    private static Map<String, Object> logSettings;


    public static int getBatchSize() {
        return BATCH_SIZE;
    }

    public static int getBatchSize(boolean isTest) throws IOException, JSONException {
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

        if (isInitialized()) {
            try (Transaction tx = gdb.beginTx()) {
                setAllParameters(tx.findNode(Label.label("ReplicationSettings"), "id", "singleton"));
                initialized = true;
            }
        }
    }
    private static void initializeJSON() throws IOException, JSONException {
        // TODO: grab and read parameters from the neo4j.conf if possible
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        if (isInitialized()) {
            configFile = new File(CONFIG_FILE_PATH + CONFIG_FILE_NAME);

            if (!configFile.exists()) {
                configFile.mkdir();
                configFile.createNewFile();
            }
            config = new JSONObject(configFile.toString());
            BATCH_SIZE = Integer.parseInt(((JSONObject) ( new JSONObject(configFile.toString())).get("configuration")).get("batchSize").toString());
        }
    }
    private static void testInitializeJSON() throws IOException, JSONException {
        // TO_DO: grab and read parameters from the neo4j.conf
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        configFile = new File(CONFIG_FILE_NAME);
        config = new JSONObject(new String(Files.readAllBytes(Paths.get(String.valueOf(configFile)))));
        JSONObject parameters = (JSONObject) config.get("configuration");
        if (parameters.has("batchSize")) BATCH_SIZE = Integer.parseInt(parameters.get("batchSize").toString());

    }

    private static void setAllParameters(Node parameters) {

        // **** UNTESTED ****
        BATCH_SIZE = Integer.parseInt(parameters.getProperty("batchSize", 100).toString());
        logSettings = new HashMap<>();
        logSettings.put("TX_LOG_FILE_DIR", parameters.getProperty("outBoundTxDir", "c:/OUTBOUND_TX"));
        logSettings.put("TX_RB_LOG_FILE_DIR" ,parameters.getProperty("outBoundRBDir", "c:/ROLLBACK_OUTBOUND_TX"));
        logSettings.put("IB_TX_LOG_FILE_DIR", parameters.getProperty("inBoundTxDir", "c:/INBOUND_TX;"));
        logSettings.put("TX_LOG_FILE_NAME_IN", parameters.getProperty("pollingDir", "inbound_tx.log"));
        logSettings.put("POLL_LOG_FILE_DIR", parameters.getProperty("pollingDir", "c:/POLLING"));
        logSettings.put("TX_LOG_FILE_NAME", parameters.getProperty("pollingDir", "outbound_tx.log"));
        logSettings.put("TX_RB_LOG_FILE_NAME", parameters.getProperty("pollingDir", "rb_outbound_tx.log"));
        logSettings.put("POLLING_LOG", parameters.getProperty("pollingDir", "tx_poll.log"));

    }
}


