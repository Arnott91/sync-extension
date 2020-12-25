package com.neo4j.sync.engine;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionHistoryManager;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.Inject;

import java.net.URI;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class ReplicationEngineTests {


    @Inject
    private ClusterFactory clusterFactory;

    private Cluster sourceCluster;
    private Cluster targetCluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);

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
    void cleanUp() throws Exception {

        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
        for (CoreClusterMember coreMember : targetCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

    }

    @Test
    public void start2Test() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());

        }

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        ReplicationEngine engine = ReplicationEngine.initialize("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress(), "neo4j", "password", defaultDB);

        engine.start2();


    }

}







