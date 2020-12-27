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

public class Configuration {

    private static final String CONFIG_FILE_PATH = "c:/CONFIG/";

    private static final String CONFIG_FILE_NAME = "replication.conf";

    private static File configFile = null;

    private static JSONObject config = null;

    private boolean useDefaults = true;

    private static int BATCH_SIZE = 100;



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




    private void Initialize() throws IOException, JSONException {
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






}
