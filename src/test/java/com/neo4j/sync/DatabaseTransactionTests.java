package com.neo4j.sync;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.listener.AuditTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.codehaus.jettison.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;

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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
    void mergeExistingNodeTest() throws Exception {
        // Do work here
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) DETACH DELETE t");
            tx.commit();
        });
    }

    @Test
    void deleteRelationshipTest() throws Exception {
        // Do work here
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
    void changeNodePropertyTest() throws Exception {
        // Do work here
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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
            tx.execute("MATCH (t:Test {uuid:'123XYZ'}) SET t.test = 'foo'");
            tx.commit();

        });
    }

    @Test
    void graphWriterInitTest() throws Exception {
        // Do work here
        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }


        JSONObject json = new JSONObject();
        GraphDatabaseService graphDb = cluster.getMemberWithAnyRole(DEFAULT_DATABASE_NAME, Role.LEADER).database(DEFAULT_DATABASE_NAME);
        GraphWriter writer = new GraphWriter(json, graphDb);
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

        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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

        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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

        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
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

        AuditTransactionEventListenerAdapter listener = new AuditTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.coreTx((db, tx) ->
        {
            tx.execute(query);
            tx.commit();

        });

    }


}
