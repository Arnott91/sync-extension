package com.neo4j.sync.auto;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityListener;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

public class AutoStart extends LifecycleAdapter {
    private final AvailabilityGuard availabilityGuard;
    private final Log log;

    public AutoStart(AutoStartExtensionFactory.Dependencies dependencies) {
        this.log = dependencies.log().getUserLog(this.getClass());
        this.availabilityGuard = dependencies.getAvailabilityGuard();
    }

    @Override
    public void start() throws Exception {
        log.info("TODO: check if auto is enabled and kick off the replication engine");
        availabilityGuard.addListener(new AvailabilityListener() {
            @Override
            public void available() {
                log.info("Do clever stuff here to check if auto mode is engaged and start the replicatio engine");
            }

            @Override
            public void unavailable() {
                log.info("Stop the replication engine");
            }
        });
    }
}
