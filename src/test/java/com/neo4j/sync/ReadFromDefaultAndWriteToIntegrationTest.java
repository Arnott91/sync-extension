package com.neo4j.sync;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.listener.AuditTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.test.extension.Inject;

import java.net.URI;

import static com.neo4j.sync.listener.AuditTransactionEventListenerAdapter.INTEGRATION_DATABASE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class ReadFromDefaultAndWriteToIntegrationTest {

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster sourceCluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);

    @BeforeEach
    void setup() throws Exception {
        sourceCluster = clusterFactory.createCluster(clusterConfig);
        sourceCluster.start();

        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, new AuditTransactionEventListenerAdapter());
        }

        // Don't need the listener on the sink cluster for this test
        sourceCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DATABASE);
        DatabaseIsReadyListener databaseEventListener = new DatabaseIsReadyListener();
        sourceCluster.awaitLeader().managementService().registerDatabaseEventListener(databaseEventListener);
        sourceCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DATABASE);
        databaseEventListener.waitUntilReady();
    }

    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration() throws Exception {
        try (Driver driver = driver(new URI("neo4j://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            session.run(("CREATE (:Person {name:'Rosa'})-[:FOLLOWS]->(:Person {name:'Karl'})"));
        }

        try (Driver driver = driver(new URI("neo4j://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(INTEGRATION_DATABASE).build());
            Result result = session.run(("MATCH p=(:Person {name:'Rosa'})-[:FOLLOWS]->(:Person {name:'Karl'}) RETURN p"));
            assertEquals(1, result.list().size());
        }
    }

    private class DatabaseIsReadyListener extends DatabaseEventListenerAdapter {
        private boolean ready = false;

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {
            this.ready = true;
        }

        public void waitUntilReady() {
            while (!ready) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
