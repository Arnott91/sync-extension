package com.neo4j.sync.engine;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;

public class ReplicationEngine {
    private final Driver driver;
    private final ScheduledExecutorService execService;
    private ScheduledFuture<?> scheduledFuture;
    private Status status = STOPPED;

    public ReplicationEngine(Driver driver) {
        this(driver, Executors.newScheduledThreadPool(1));
    }

    ReplicationEngine(Driver driver, ScheduledExecutorService executorService) {
        this.driver = driver;
        this.execService = executorService;
    }

    public synchronized void start() {
        if (status == RUNNING) {
            return;
        }
        scheduledFuture = execService.scheduleAtFixedRate(() -> {
            // TODO: change this for real cypher that pulls from the remote database
            Result run = driver.session().run("MATCH (n) RETURN (n)");
            run.forEachRemaining(System.out::println);

        }, 0, 60L, TimeUnit.SECONDS);
        status = RUNNING;
    }

    public void stop() {
        if (status == STOPPED) {
            return;
        }
        scheduledFuture.cancel(true);
        status = Status.STOPPED;
    }

    public Status status() {
        return status;
    }

    public enum Status {RUNNING, STOPPED}
}
