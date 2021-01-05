package com.neo4j.sync;

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
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class TransactionRecordPruningTests {

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

    private String createTRQuery = "CREATE (tr:TransactionRecord) SET tr.uuid = randomUUID(), tr.timeCreated = ";
    private String countAllTRs = "MATCH (tr:TransactionRecord) RETURN COUNT(tr) as trCount;";
    private String countAllTRsWithTxData = "MATCH (tr:TransactionRecord) WHERE EXISTS(tr.transactionData) " +
                                            "RETURN COUNT(tr) as trCount;";
    private  String pruneTRQuery = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated < %d DETACH DELETE tr";

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

        // Don't need the listener on the sink cluster for this test
        // sourceCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DATABASE);
        // DatabaseIsReadyListener integrationDatabaseListener = new DatabaseIsReadyListener();
        //sourceCluster.awaitLeader().managementService().registerDatabaseEventListener(integrationDatabaseListener);
        //sourceCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DATABASE);
        //integrationDatabaseListener.waitUntilReady();
//        System.out.println("Waiting awhile for database creation");
//        Thread.sleep(100000l);
//        System.out.println("Done waiting");
    }

    @AfterEach
    void cleanUp() throws Exception {

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

            assertEquals(6,(countResult.next().get("trCount").asInt()));

            // now let's get rid of all TransactionRecord nodes with a timeCreated older than six days from now.


            session.run(String.format(pruneTRQuery,getThreeDaysAgo()));

            Result countPruneResult = session.run(countAllTRs);

            // so, above we created today, yesterday, two days ago, three days ago, four days ago and five days ago
            // should only be four left after we prune.

            assertEquals(4,countPruneResult.next().get("trCount").asInt());


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

            assertEquals(0,(countResult.next().get("trCount").asInt()));

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
