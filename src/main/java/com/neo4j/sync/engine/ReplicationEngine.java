package com.neo4j.sync.engine;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ReplicationEngine {
    private final Driver driver;
    private ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> scheduledFuture;

    public ReplicationEngine(Driver driver ) {
        this.driver = driver;
    }

    public void start() {
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            driver.session().run("MATCH (n) RETURN (n)");

        }, 0, 60L, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduledFuture.cancel(true);
    }
}
