package com.neo4j.sync;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.procedures.StartAndStopReplicationProcedures;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.junit.jupiter.causal_cluster.*;
import org.neo4j.test.jar.JarBuilder;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.shaded.com.google.common.io.Files;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@NeedsCausalCluster(neo4jVersion = "4.2", createMultipleClusters = true)
public class EndToEndContainerIT {
    private static final String myPlugin = "myPlugin.jar";

    private static final AuthToken authToken = AuthTokens.basic("neo4j", "password");

    public static final String INTEGRATION_DB_NAME_1 = "IntegrationdbOne";
    public static final String INTEGRATION_DB_NAME_2 = "IntegrationdbTwo";

    @CausalCluster
    private static Neo4jCluster clusterOne;
    private String clusterOneInternalAddress;

    @CausalCluster
    private static Neo4jCluster clusterTwo;
    private String clusterTwoInternalAddress;

    private SessionConfig sessionConfigOne;
    private SessionConfig sessionConfigTwo;

    @CoreModifier
    private static Neo4jContainer<?> configure(Neo4jContainer<?> input) throws IOException {

        // dump the current (test time) classpath into the container
        input = DeveloperWorkflow.configureNeo4jWithCurrentClasspath(input);

        // This relies on internal implementation details but we are in advanced territory here so go figure
        int serverIndex = input.getNetworkAliases().stream()
                .filter(s -> s.startsWith("neo4j"))
                .map(s -> s.replace("neo4j", ""))
                .map(s -> Integer.parseInt(s) - 1)
                .findFirst().get();
        int clusterIndex = Math.floorDiv(serverIndex, ClusterFactory.MAXIMUM_NUMBER_OF_SERVERS_ALLOWED);


        var pluginsDir = Files.createTempDir();
        var myPluginJar = pluginsDir.toPath().resolve(myPlugin);

        new JarBuilder().createJarFor(myPluginJar, StartAndStopReplicationProcedures.class, ReplicationEngine.class,
                StartAndStopReplicationProcedures.Output.class);

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

    @BeforeAll
    public void setupTheIntegrationDB() {

        sessionConfigOne = SessionConfig.builder().withDatabase(INTEGRATION_DB_NAME_1).build();
        sessionConfigTwo = SessionConfig.builder().withDatabase(INTEGRATION_DB_NAME_2).build();

        try (
                Driver coreDriver = GraphDatabase.driver(clusterOne.getURI(), authToken);
                Session systemSession = coreDriver.session(SessionConfig.forDatabase("system"));
                Session session = coreDriver.session()) {

            systemSession.run(String.format("CREATE DATABASE %s IF NOT EXISTS WAIT", INTEGRATION_DB_NAME_1))
                    .consume();
            var hostname = session.run("CALL dbms.listConfig('default_advertised_address')").stream().findFirst().get().get("value").asString();
            clusterOneInternalAddress = "bolt://" + hostname + ":7687";
        }

        try (
                Driver coreDriver = GraphDatabase.driver(clusterTwo.getURI(), authToken);
                Session systemSession = coreDriver.session(SessionConfig.forDatabase("system"));
                Session session = coreDriver.session()) {

            systemSession.run(String.format("CREATE DATABASE %s IF NOT EXISTS WAIT", INTEGRATION_DB_NAME_2))
                    .consume();
            var hostname = session.run("CALL dbms.listConfig('default_advertised_address')").stream().findFirst().get().get("value").asString();
            clusterTwoInternalAddress = "bolt://" + hostname + ":7687";
        }
    }

    @Test
    public void shouldStartAndStopReplicationOnBothClusters() {

        try (Driver coreDriver = GraphDatabase.driver(clusterOne.getURI(), authToken);
             Session session = coreDriver.session(sessionConfigOne)) {

            session.run(format("CALL startReplication('%s', '%s', '%s')", clusterTwoInternalAddress, "neo4j", "password")).consume();

            Result res = session.run("CALL replicationStatus");
            var statuses = res.stream().map(r -> r.get("status").asString()).collect(Collectors.toList());
            assertThat(statuses).containsOnly("running");

            session.run("CALL stopReplication()").consume();
            res = session.run("CALL replicationStatus");
            statuses = res.stream().map(r -> r.get("status").asString()).collect(Collectors.toList());
            assertThat(statuses).containsOnly("stopped");
        }

        try (Driver coreDriver = GraphDatabase.driver(clusterTwo.getURI(), authToken);
             Session session = coreDriver.session(sessionConfigTwo)) {

            session.run(format("CALL startReplication('%s', '%s', '%s')", clusterOneInternalAddress, "neo4j", "password"));

            Result res = session.run("CALL replicationStatus");
            var statuses = res.stream().map(r -> r.get("status").asString()).collect(Collectors.toList());
            assertThat(statuses).containsOnly("running");

            session.run("CALL stopReplication()").consume();
            res = session.run("CALL replicationStatus");
            statuses = res.stream().map(r -> r.get("status").asString()).collect(Collectors.toList());
            assertThat(statuses).containsOnly("stopped");
        }
    }
}
