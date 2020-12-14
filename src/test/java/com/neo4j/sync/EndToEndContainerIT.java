package com.neo4j.sync;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.procedures.StartAndStopReplicationProcedures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.junit.jupiter.causal_cluster.*;
import org.neo4j.test.jar.JarBuilder;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.shaded.com.google.common.io.Files;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.UUID;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@NeedsCausalCluster(neo4jVersion = "4.2", createMultipleClusters = true)
public class EndToEndContainerIT {
    private static final String myPlugin = "myPlugin.jar";

    private static final AuthToken authToken = AuthTokens.basic("neo4j", "password");

    public static final String INTEGRATION_DB_NAME = format("Integrationdb-%s", UUID.randomUUID());

    @CausalCluster
    private static Neo4jCluster clusterOne;

    @CausalCluster
    private static Neo4jCluster clusterTwo;

    @CoreModifier
    private static Neo4jContainer<?> configure(Neo4jContainer<?> input) throws IOException {

        // dump the current (test time) classpath into the container
        input = DeveloperWorkflow.configureNeo4jWithCurrentClasspath( input );

        // This relies on internal implementation details but we are in advanced territory here so go figure
        int serverIndex = input.getNetworkAliases().stream()
                .filter(s -> s.startsWith("neo4j"))
                .map(s -> s.replace("neo4j", ""))
                .map(s -> Integer.parseInt(s) - 1)
                .findFirst().get();
        int clusterIndex = Math.floorDiv(serverIndex, ClusterFactory.MAXIMUM_NUMBER_OF_SERVERS_ALLOWED);


        var pluginsDir = Files.createTempDir();
        var myPluginJar = pluginsDir.toPath().resolve(myPlugin);

        new JarBuilder().createJarFor(myPluginJar, StartAndStopReplicationProcedures.class, ReplicationEngine.class);

        input = input.withNeo4jConfig("dbms.security.procedures.unrestricted", "*")
                .withCopyFileToContainer(MountableFile.forHostPath(myPluginJar), "/plugins/myPlugin.jar");

        switch (clusterIndex) {
            case 0:
                return input.withNeo4jConfig("my.cluster_one.config", "foo");
            case 1:
                return input.withNeo4jConfig("my.cluster_two.config", "bar");
            default:
                throw new IllegalStateException("Unexpected cluster index: " + clusterIndex);
        }
    }

    @BeforeEach
    void setup() throws Exception {

    }

    @Test
    public void shouldPullFromRemoteDatabaseAndStoreInLocalDatabase() throws Exception {

        try (Driver coreDriver = GraphDatabase.driver(clusterOne.getURI(), authToken)) {
            Session session = coreDriver.session();
            Result res = session.run("CALL dbms.procedures() YIELD name, signature RETURN name, signature");

            // Then the procedure from the plugin is listed
            assertTrue(res.stream().anyMatch(x -> x.get("name").asString().contains("startReplication")),
                    "Missing procedure provided by our plugin");
        }

        try (Driver coreDriver = GraphDatabase.driver(clusterTwo.getURI(), authToken)) {
            Session session = coreDriver.session();
            Result res = session.run("CALL dbms.procedures() YIELD name, signature RETURN name, signature");

            // Then the procedure from the plugin is listed
            assertTrue(res.stream().anyMatch(x -> x.get("name").asString().contains("startReplication")),
                    "Missing procedure provided by our plugin");
        }

//        String sourceBoltAddress = "neo4j://" + sourceCluster.awaitLeader().boltAdvertisedAddress();
//        System.out.println("sourceBoltAddress = " + sourceBoltAddress);
//
//
//        sinkCluster.coreTx(INTEGRATION_DB_NAME, (db, tx) ->
//        {
//            tx.execute(format("CALL startReplication('%s', '%s', '%s')",
//                    sourceBoltAddress, "neo4j", "password"));
//
//            tx.commit();
//        });
    }
}
