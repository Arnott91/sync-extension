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

import static java.lang.String.format;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@NeedsCausalCluster(neo4jVersion = "4.2", createMultipleClusters = true)
public class EndToEndContainerIT {
    private static final String myPlugin = "myPlugin.jar";

    private static final AuthToken authToken = AuthTokens.basic("neo4j", "password");

    public static final String INTEGRATION_DB_NAME = "Integrationdb";

    @CausalCluster
    private static Neo4jCluster clusterOne;

    @CausalCluster
    private static Neo4jCluster clusterTwo;
    private SessionConfig sessionConfig;

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

    @BeforeEach
    public void setupTheIntegrationDB() {

        sessionConfig = SessionConfig.builder().withDatabase(INTEGRATION_DB_NAME).build();

        // TODO: need to figure out how to get the dockerised address
        try (Driver coreDriver = GraphDatabase.driver(clusterOne.getURI(), authToken)) {
            Session session = coreDriver.session(sessionConfig);
            session.run("MATCH (n) DETACH DELETE (n)");
            Result result = session.run("CREATE (:Person {name:'Karl'})-[:FOLLOWS]->(:Person {name:'Rosa'})");
            result.stream().forEach(System.out::println);
        }
    }

    @Test
    public void shouldPullFromRemoteDatabaseAndStoreInLocalDatabase() {


        try (Driver coreDriver = GraphDatabase.driver(clusterOne.getURI(), authToken)) {
            Session session = coreDriver.session(sessionConfig);
            session.run(format("CALL startReplication('%s', '%s', '%s')", clusterTwo.getURI(), "neo4j", "password"));

//            Result res = session.run("CALL replicationStatus");
//            res.stream().forEach(System.out::println);
//
//            session.run("CALL stopReplication()");
//                res = session.run("CALL replicationStatus");
//            res.stream().forEach(System.out::println);
        }


        try (Driver coreDriver = GraphDatabase.driver(clusterTwo.getURI(), authToken)) {
            Session session = coreDriver.session(sessionConfig);
            session.run(format("CALL startReplication('%s', '%s', '%s')", clusterOne.getURI(), "neo4j", "password"));

//            Result res = session.run("CALL replicationStatus");
//            res.stream().forEach(System.out::println);
//
//            session.run("CALL stopReplication()");
//                res = session.run("CALL replicationStatus");
//            res.stream().forEach(System.out::println);
        }


//        try (Driver coreDriver = GraphDatabase.driver(clusterTwo.getURI(), authToken)) {
//            Session session = coreDriver.session();
//            Result res = session.run("CALL dbms.procedures() YIELD name, signature RETURN name, signature");
//
//            // Then the procedure from the plugin is listed
//            assertTrue(res.stream().anyMatch(x -> x.get("name").asString().contains("startReplication")),
//                    "Missing procedure provided by our plugin");
//        }
    }
}
