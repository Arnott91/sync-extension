package com.neo4j.sync.engine;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.extension.Inject;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class ReplicationEngineTests {


    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);
    @Inject
    private ClusterFactory clusterFactory;
    private Cluster sourceCluster;
    private Cluster targetCluster;
    private CaptureTransactionEventListenerAdapter listener;

    @BeforeEach
    void setup() throws Exception {
        sourceCluster = clusterFactory.createCluster(clusterConfig);
        sourceCluster.start();

        targetCluster = clusterFactory.createCluster(clusterConfig);
        targetCluster.start();

        listener = new CaptureTransactionEventListenerAdapter();

        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        for (CoreClusterMember coreMember : targetCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

    }

    @AfterEach
    void cleanUp() {

        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
        for (CoreClusterMember coreMember : targetCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }

    @Test
    public void pollingTest1() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());
        }

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        ReplicationEngine engine = ReplicationEngine.initialize("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress(), "neo4j", "password", defaultDB);

        engine.testPolling(3);
    }

    @Test
    public void pollingTest2() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());

            result = session.run("MATCH (p:Person {uuid:'Rosa'}) DETACH DELETE p");
            assertEquals(0, result.list().size());
        }

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        ReplicationEngine engine = ReplicationEngine.initialize("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress(), "neo4j", "password", defaultDB);

        engine.testPolling(2);
    }

    @Test
    public void pollingTest3() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            // create test statement record
            String srQuery = "CREATE (st:TransactionRecord:StatementRecord {uuid:'12314124123'}) " +
                    "SET st.timeCreated = 123123, st.transactionStatement = 'CREATE INDEX FOR (p:Person) ON (p.name)' " +
                    "SET st.transactionData = '{\"statement\":\"true\"}'" +
                    "RETURN count(st)";

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            assertEquals(1, session.run(srQuery).list().size());
        }


        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        ReplicationEngine engine = ReplicationEngine.initialize("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress(), "neo4j", "password", defaultDB);

        engine.testPolling(2);

        fail("TODO: assert that data was written locally");
    }

    @Test
    public void pollingTest4() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());

            result = session.run("MATCH (p:Person {uuid:'Rosa'}) DETACH DELETE p");
            assertEquals(0, result.list().size());
        }

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        ReplicationEngine engine = ReplicationEngine.initialize("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress(), "neo4j", "password", defaultDB);

        engine.testPolling(2);
    }
}







