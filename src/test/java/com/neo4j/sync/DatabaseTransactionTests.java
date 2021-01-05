package com.neo4j.sync;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.fabric.shaded.driver.GraphDatabase;
import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionDataParser;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.test.extension.Inject;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class DatabaseTransactionTests {
    @Inject



    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);

    @BeforeEach
    void setup() throws Exception {
        cluster = clusterFactory.createCluster(clusterConfig);
        cluster.start();
    }


    @Test
    void createNodesTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})");
            tx.execute("CREATE (t2:Test {uuid:'XZY123'})");
            tx.commit();
        });
    }

    @Test
    void mergeNewNodeTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MERGE (t:Test {uuid:'123XYZ'})");
            tx.commit();
        });
    }

    @Test
    void addNodePropertiesTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})");
            tx.commit();
        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MERGE (t:Test {uuid:'123XYZ'}) SET t.name='foo'");
            tx.commit();
        });
    }

    @Test
    void mergeExistingNodeTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})");
            tx.commit();
        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MERGE (t:Test {uuid:'123XYZ'})");
            tx.commit();
        });
    }

    @Test
    void createNodesAndRelationshipTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'})");
            tx.commit();
        });


    }

    @Test
    void createMultipleRelationshipsTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'})");
            tx.execute("MERGE (t:Test {uuid:'123XYZ'})-[:LIKES]->(t2:Test {uuid:'001'})");
            tx.commit();
        });


    }

    @Test
    void addRelationshipsPropertiesTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'})");
            tx.execute("MERGE (t:Test {uuid:'123XYZ'})-[:LIKES {weight:123}]->(t2:Test {uuid:'001'})");
            tx.commit();
        });


    }


    @Test
    void deleteRelationshipsTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'})");
            tx.commit();


        });

        cluster.coreTx((db, tx) ->
        {

            tx.execute("MATCH (t:Test {uuid:'123XYZ'})-[r:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'}) DELETE r");
            tx.commit();

        });


    }

    @Test
    void deleteExistingNodeTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})");
            tx.commit();
        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) DELETE t");
            tx.commit();
        });
    }

    @Test
    void deleteDetachExistingNodeTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO {uuid:123}]->(t2:Test {uuid:'XYZ123'})");
            tx.commit();
        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) DETACH DELETE t");
            tx.commit();
        });
    }

    @Test
    void deleteRelationshipTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO {uuid:123}]->(t2:Test {uuid:'XYZ123'})");
            tx.commit();
        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'})-[r:CONNECTED_TO]->(t2:Test {uuid:'XYZ123'}) DELETE r");
            tx.commit();
        });
    }

    @Test
    void changeNodePropertyTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
//        for (CoreClusterMember coreMember : cluster.coreMembers()) {
//            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
//        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})");
            tx.commit();

        });
        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) SET t.test = 'foo'");
            tx.commit();

        });
    }

    @Test
    void removeNodePropertyTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'}) SET t.test = 'foo'");
            tx.commit();

        });
        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) REMOVE t.test");
            tx.commit();

        });
    }

    @Test
    void NodePropertyChangeTest2() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'}) SET t.test = 'foo'");
            tx.commit();

        });
        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) SET t.test= 'bar'");
            tx.commit();

        });
    }

    @Test
    void RelationPropertyChangeTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test {uuid:'123XYZ'})-[:CONNECTED_TO {weight:1}]->(t2:Test {uuid:'XYZ123'})");
            tx.commit();

        });
        cluster.coreTx((db, tx) ->
        {
            tx.execute("MATCH (t:Test {uuid:'123XYZ'})-[r:CONNECTED_TO {weight:1}]->(t2:Test {uuid:'XYZ123'}) SET r.weight = 2");
            tx.commit();

        });
    }

    // BEGIN - Federos-specific tests

    @Test
    void federosQuery1Test() throws Exception {

        String query = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr1.federos.com\", DeviceID: 1, TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr1.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr1.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.1\", UUID: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.1\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm};";

        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute(query);
            tx.commit();
        });
    }


    @Test
    void federosQuery2Test() throws Exception {

        String query = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", UUID: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm};";

        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute(query);
            tx.commit();
        });
    }

    @Test
    void federosQuery3Test() throws Exception {

        String query = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Interface {Name: 'usfix-rtr1.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.1\", UUID: randomUUID()}\n" +
                "ON MATCH SET v1 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.1\"}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", UUID: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:ConnectsInterface]-(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm};";

        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute(query);
            tx.commit();
        });

    }

    @Test
    void federosQuery4Test() throws Exception {

        String query = " WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {Name: \"usfix-rtr1.federos.com\", DNSName: \"usfix-rtr1.federos.com\", DeviceID: 1, TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET v1 += {Name: \"usfix-rtr1.federos.com\", DNSName: \"usfix-rtr1.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {Name: \"usfix-rtr2.federos.com\", DNSName: \"usfix-rtr2.federos.com\", DeviceID: 2, TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET v2 += {Name: \"usfix-rtr2.federos.com\", DNSName: \"usfix-rtr2.federos.com\", DeviceID: 2, TimestampModified: tm}\n" +
                "        MERGE (v1)-[e:ConnectsNeighbor]-(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, UUID: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm};";

        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute(query);
            tx.commit();
        });
    }

    @Test
    void noLabelsTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t {uuid:'123XYZ'})-[:CONNECTED_TO]->(t2 {uuid:'XYZ123'})");
            tx.commit();
        });


    }

    @Test
    void noPropertiesTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (t:Test)-[:CONNECTED_TO]->(t2:Test)");
            tx.commit();
        });


    }

    @Test
    void addUserTest1() throws Exception {
        // passed
        assertNotNull(cluster);

        GraphDatabaseAPI graphDb = cluster.awaitLeader().database("SYSTEM");

        graphDb.executeTransactionally("CREATE USER jake IF NOT EXISTS SET PASSWORD 'xyz'");

        Transaction tx = graphDb.beginTx();

        try (Result result = tx.execute("SHOW USERS YIELD user as username");) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                for (String key : result.columns()) {
                    System.out.printf("%s = %s%n", key, row.get(key));
                }
            }
        }
    }


    @Test
    void createIndexTest() throws Exception {
        // Do work here
        CaptureTransactionEventListenerAdapter listener = new CaptureTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE INDEX my_index FOR (p:Person) ON (p.age)");
            tx.commit();

        });

        cluster.coreTx((db, tx) ->
        {
            tx.execute("CREATE (p:Person {id:1}) set p.age = 22");
            tx.commit();

        });




    }
}