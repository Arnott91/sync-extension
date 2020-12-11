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
import org.neo4j.test.extension.Inject;

import java.util.UUID;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class EndToEndIT {

    public static final String INTEGRATION_DB_NAME = format("Integrationdb-%s", UUID.randomUUID());

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster sourceCluster;
    private Cluster sinkCluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);

    @BeforeEach
    void setup() throws Exception {

        sourceCluster = clusterFactory.createCluster(clusterConfig);
        sourceCluster.start();

        sinkCluster = clusterFactory.createCluster(clusterConfig);
        sinkCluster.start();

        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, new AuditTransactionEventListenerAdapter());
            // dependencies.procedures().registerProcedure

        }

        // Don't need the listener on the sink cluster for this test

        sourceCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DB_NAME);
        sourceCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DB_NAME);
        sinkCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DB_NAME);
        sinkCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DB_NAME);

        // Create some data
        sourceCluster.coreTx(INTEGRATION_DB_NAME, (db, tx) ->
        {
            tx.execute("MERGE (:Person {name:'Rosa'})-[:FOLLOWS]->(:Person {name:'Karl'})");
            tx.commit();
        });
    }

    @Test
    public void shouldPullFromRemoteDatabaseAndStoreInLocalDatabase() throws Exception {

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
