package com.neo4j.sync.start;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

public class Startup extends LifecycleAdapter {
    private final AvailabilityGuard availabilityGuard;
    private final Log log;

    private static final Object lock = new Object();

    private static StartupExtensionFactory.Dependencies theDependencies;

    public static GraphDatabaseService getDatabase(String databaseName) {
        return theDependencies.getDatabaseManagementService().database(databaseName);
    }

    public Startup(StartupExtensionFactory.Dependencies dependencies) {
        this.log = dependencies.log().getUserLog(this.getClass());
        this.availabilityGuard = dependencies.getAvailabilityGuard();

        synchronized (lock) {
            if (theDependencies == null) {
                theDependencies = dependencies;
            }
        }
    }

    @Override
    public void start() throws Exception {
        log.info("TODO: check if auto is enabled and kick off the replication engine");
        availabilityGuard.addListener(new AvailabilityListener() {
            @Override
            public void available() {
                if(autoRestart()) {
                    log.info("Do clever stuff here to check if auto mode is engaged and start the replication engine");
                }
            }

            @Override
            public void unavailable() {
                // Do nothing, the dbms is not available.
            }
        });
    }

    private boolean autoRestart() {
        // TODO: access the DB for the auto restart config and return boolean
        return false;
    }
}
