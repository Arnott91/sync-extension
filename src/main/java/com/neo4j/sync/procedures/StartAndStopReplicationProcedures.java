package com.neo4j.sync.procedures;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.engine.TransactionFileLogger;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.sql.Date;
import java.util.stream.Stream;

public class StartAndStopReplicationProcedures {
    @Context
    public Log log;

    @Procedure(name = "startReplication", mode = Mode.WRITE)
    @Description("starts the bilateral replication engine on this server.")
    public synchronized void startReplication(
            @Name(value = "remoteDatabaseURI") String remoteDatabaseURI,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {

            Driver driver = GraphDatabase.driver( remoteDatabaseURI, AuthTokens.basic( username, password ) );
            driver.verifyConnectivity();
            ReplicationEngine.initialize(remoteDatabaseURI, username, password).start();

        log.info("Replication from %s started.", remoteDatabaseURI);
    }

    @Procedure(name = "stopReplication", mode = Mode.WRITE)
    @Description("stops the bilateral replication engine on this server.")
    public void stopReplication() {
        ReplicationEngine.instance().stop();
        log.info("Replication stopped.");
    }

    @Procedure(name = "replicationStatus", mode = Mode.WRITE)
    @Description("returns whether the replication engine is running on this server.")
    public Stream<Output> replicationStatus() {
        Output output = new Output(ReplicationEngine.instance().status());
        try {
            TransactionFileLogger.AppendPollingLog("Procedure starting: " + new Date(System.currentTimeMillis()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.of(output);
    }

    public static class Output {
        public final String status;

        public Output(ReplicationEngine.Status status) {
            this.status = status.toString().toLowerCase();
        }
    }
}
