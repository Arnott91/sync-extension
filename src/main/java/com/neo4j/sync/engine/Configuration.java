package com.neo4j.sync.engine;

public class Configuration {

    private static String BATCH_SIZE;

    public static String getBatchSize() {
        return BATCH_SIZE;
    }


    private void Initialize() {
        // TO_DO: grab and read parameters from the neo4j.conf
        // OTHERWISE:  // grab the internal config object and get replication specific parameters.
        // Might as well stub out initialize with our own config file.
        // why? because of time.  we can deploy with replication.conf in the import directory.

    }
}
