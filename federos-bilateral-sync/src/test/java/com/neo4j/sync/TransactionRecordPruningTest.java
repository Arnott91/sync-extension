package com.neo4j.sync;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.test.extension.Inject;

import java.net.URI;
import java.util.Calendar;

import static org.junit.jupiter.api.Assertions.*;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class TransactionRecordPruningTest {

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

    private final String createTRQuery = "CREATE (tr:TransactionRecord) SET tr.uuid = randomUUID(), tr.timeCreated = ";
    private final String countAllTRs = "MATCH (tr:TransactionRecord) RETURN COUNT(tr) as trCount;";
    private final String countAllTRsWithTxData = "MATCH (tr:TransactionRecord) WHERE EXISTS(tr.transactionData) " +
            "RETURN COUNT(tr) as trCount;";
    private final String pruneTRQuery = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated < %d DETACH DELETE tr";

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
    }


    @Test
    public void pruneOldTransactionRecordsTest1() throws Exception {

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());

            // first, let's write six transaction records going back six days.

            for (int i = 0; i < 6; i++) {
                session.run(createTRQuery + daysAgo(i));
            }
            Result countResult = session.run(countAllTRs);

            // make sure they are in the database

            assertEquals(6, (countResult.next().get("trCount").asInt()));

            // now let's get rid of all TransactionRecord nodes with a timeCreated older than six days from now.

            session.run(String.format(pruneTRQuery, getThreeDaysAgo()));

            Result countPruneResult = session.run(countAllTRs);

            // so, above we created today, yesterday, two days ago, three days ago, four days ago and five days ago
            // should only be four left after we prune.

            assertEquals(4, countPruneResult.next().get("trCount").asInt());
        }
    }


    @Test
    public void pruneOldTransactionRecordsTest2() throws Exception {
        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());

            // first, let's write six transaction records going back six days.

            for (int i = 0; i < 6; i++) {
                session.run(createTRQuery + daysAgo(i));
            }
            Result countResult = session.run(countAllTRsWithTxData);

            // make sure there are no replicas in the database

            assertEquals(0, (countResult.next().get("trCount").asInt()));

            // now let's get rid of all TransactionRecord nodes with a timeCreated older than six days from now.
        }
    }

    private long daysAgo(int daysPast) {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -daysPast);
        return (cal.getTime()).getTime();
    }

    private long getThreeDaysAgo() {
        return daysAgo(3);
    }
}
