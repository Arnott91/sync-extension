package com.neo4j.sync.procedures;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.engine.TransactionFileLogger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.IOException;
import java.sql.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Protocol is as follows.
 * <p>
 * the start procedure accepts the URI and authentication parameters, or in the case of a virtual URI, also accepts
 * hostnames and uses the address resolver to return a driver.  The procedure then initializes the
 * replication engine with the parameters and the driver.  The stop procedure simple calls the stop method
 * of the engine and the status returns a status object with runtime information.
 * </p>
 *
 * @author Chris Upkes
 * @author Jim Webber
 */

public class StartAndStopReplicationProcedures {
    @Context
    public Log log;

    @Context
    public GraphDatabaseService gds;

    // the idea here would be to provide a virtual URI along with the username
    // and password.  If we are using the JWT functionality we could
    // still leave the password parameter in place and fill it with
    // a dummy string or random nonsense.
    // currently, we don't pass polling interval to the replication engine through
    // the stored procedure, although we could add that as a parameter.
    @Procedure(name = "startReplication", mode = Mode.WRITE)
    @Description("starts the bilateral replication engine on this server.")
    public synchronized void startReplication(
            @Name(value = "virtualRemoteDatabaseURI1") String virtualRemoteDatabaseURI1,
            @Name(value = "hostName1") String hostName1,
            @Name(value = "hostName2") String hostName2,
            @Name(value = "hostName3") String hostName3,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {

        Set<String> hostNames = new HashSet<>();
        hostNames.add(hostName1);
        hostNames.add(hostName2);
        hostNames.add(hostName3);

        try {
            ReplicationEngine.initialize(virtualRemoteDatabaseURI1, username, password, gds, hostNames).start();

            log.info("Replication from %s started.", virtualRemoteDatabaseURI1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    DA - added for testing
     */
    @Procedure(name = "startGraphReplication", mode = Mode.WRITE)
    @Description("starts the bilateral replication engine on this server.")
    public synchronized void startGraphReplication(
            @Name(value = "virtualRemoteDatabaseURI1") String virtualRemoteDatabaseURI1,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {

        try {
            ReplicationEngine.initialize(virtualRemoteDatabaseURI1, username, password, gds).start();

            log.info("Graph replication from %s started.", virtualRemoteDatabaseURI1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Procedure(name = "startSchemaReplication", mode = Mode.SCHEMA)
    @Description("starts the bilateral replication engine on this server.")
    public synchronized void startSchemaReplication(
            @Name(value = "virtualRemoteDatabaseURI1") String virtualRemoteDatabaseURI1,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {

        try {
            ReplicationEngine.initializeStmtRepl(virtualRemoteDatabaseURI1, username, password, gds).startStmtRepl();
            log.info("Statement replication from %s started.", virtualRemoteDatabaseURI1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // we can possibly use this as an alternative stored start proc signature.
    public synchronized void startReplication(
            @Name(value = "virtualRemoteDatabaseURI1") String virtualRemoteDatabaseURI1,
            @Name(value = "hostNames") Set<String> hostNames,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {


        try {
            ReplicationEngine.initialize(virtualRemoteDatabaseURI1, username, password, hostNames).start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("Replication from %s started.", virtualRemoteDatabaseURI1);
    }

    // virtual URI example:  x.example.com is the virtual URI
    // hostnames are a.example.com, b.example.com, c.example.com
    public synchronized void startReplication(
            @Name(value = "virtualRemoteDatabaseURI1") String virtualRemoteDatabaseURI1,
            @Name(value = "hostNames") String[] hostNames,
            @Name(value = "username") String username,
            @Name(value = "password") String password) {


        try {
            ReplicationEngine.initialize(virtualRemoteDatabaseURI1, username, password, Set.of(hostNames)).start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("Replication from %s started.", virtualRemoteDatabaseURI1);
    }

    @Procedure(name = "stopGraphReplication", mode = Mode.WRITE)
    @Description("stops the bilateral replication engine on this server.")
    public void stopGraphReplication() {
        ReplicationEngine.instance().stop();
        log.info("Graph replication stopped.");
    }

    @Procedure(name = "stopSchemaReplication", mode = Mode.SCHEMA)
    @Description("stops the bilateral replication engine on this server.")
    public void stopSchemaReplication() {
        ReplicationEngine.stmtReplInstance().stopStmtRepl();
        log.info("Schema replication stopped.");
    }

    @Procedure(name = "graphReplicationStatus", mode = Mode.WRITE)
    @Description("returns whether the replication engine is running on this server.")
    public Stream<Output> graphReplicationStatus() {
        Output output = new Output(ReplicationEngine.instance().status());
        try {
            TransactionFileLogger.appendPollingLog("Procedure starting: " + new Date(System.currentTimeMillis()), log);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.of(output);
    }

    @Procedure(name = "schemaReplicationStatus", mode = Mode.SCHEMA)
    @Description("returns whether the replication engine is running on this server.")
    public Stream<Output> schemaReplicationStatus() {
        Output output = new Output(ReplicationEngine.stmtReplInstance().stmtReplStatus());
        try {
            TransactionFileLogger.appendPollingLog("Procedure starting: " + new Date(System.currentTimeMillis()), log);
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
