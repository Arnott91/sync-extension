package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.internal.id.ScanOnOpenOverwritingIdGeneratorFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.*;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class Configuration {



    private static final String CONFIG_FILE_PATH = "c:/CONFIG/";

    private static final String CONFIG_FILE_NAME = "replication.conf";

    private static File configFile = null;

    private static JSONObject config = null;

    private boolean useDefaults = true;

    private static boolean initialized = false;

    private static int BATCH_SIZE = 100;

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


    public static void InitializeFromDB(GraphDatabaseService gdb){

        try (Transaction tx = gdb.beginTx()) {
            Node settings = tx.findNode(Label.label("ReplicationSettings"), "id", "singleton");
            setAllParameters(settings);
            initialized = true;

        }



    }

    private static void initializeJSON() throws IOException, JSONException {
        // TO_DO: grab and read parameters from the neo4j.conf
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        File logFile = new File(CONFIG_FILE_PATH + CONFIG_FILE_NAME);
        if (!logFile.exists()) {
            logFile.mkdir();
            logFile.createNewFile();
        }

        configFile = logFile;
        config = new JSONObject(logFile.toString());
        JSONObject parameters = (JSONObject) config.get("configuration");
        BATCH_SIZE = (int) parameters.get("batchSize");





    }

    private static void testInitializeJSON() throws IOException, JSONException {
        // TO_DO: grab and read parameters from the neo4j.conf
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

        File logFile = new File(CONFIG_FILE_NAME);


        configFile = logFile;
        logFile.setReadable(true);
        String data = "";
        data = new String(Files.readAllBytes(Paths.get(String.valueOf(logFile))));

        config = new JSONObject(data);
        JSONObject parameters = (JSONObject) config.get("configuration");
        if (parameters.has("batchSize")) BATCH_SIZE = Integer.valueOf(parameters.get("batchSize").toString());





    }

    private static void setAllParameters(Node parameters) {



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


