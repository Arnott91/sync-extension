package com.neo4j.sync;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.engine.GraphWriter;
import com.neo4j.sync.engine.TransactionHistoryManager;
import com.neo4j.sync.listener.AuditTransactionEventListenerAdapter;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class ReadFromDefaultAndWriteToIntegrationTest {

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster sourceCluster;
    private Cluster targetCluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);

    private AuditTransactionEventListenerAdapter listener;

    @BeforeEach
    void setup() throws Exception {
        sourceCluster = clusterFactory.createCluster(clusterConfig);
        sourceCluster.start();

        targetCluster = clusterFactory.createCluster(clusterConfig);
        targetCluster.start();

        listener = new AuditTransactionEventListenerAdapter();

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
    public void listenerShouldListenToDefaultAndWriteToIntegration() throws Exception {
        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("MATCH (tr:TransactionRecord) RETURN tr");
            assertEquals(1, result.list().size());

        }
    }

    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration2() throws Exception {
        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("MATCH (tr:TransactionRecord) RETURN tr.transactionData as data");
            //assertEquals(1, result.list().size());
            Record record = result.single();
            Value transactionData = record.get("data");
            System.out.println(transactionData);

            CoreClusterMember leader = targetCluster.awaitLeader();
            GraphDatabaseFacade defaultDB = leader.defaultDatabase();


            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), defaultDB);
            graphWriter.executeCRUDOperation();

            org.neo4j.graphdb.Transaction tx = defaultDB.beginTx();
            Iterable<Node> newNodes = () -> tx.findNodes(Label.label("Person"));
            Assertions.assertTrue(newNodes.iterator().hasNext());
            newNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Person"))));
            newNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            newNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("FOLLOWS"))));

            tx.commit();


        }
    }

    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration3() throws Exception {


        String federosQuery1 = "WITH timestamp() AS tm\n" +
                "        CREATE (v1:Device {Name: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "SET v1 += {DNSName: \"usfix-rtr1.federos.com\", DeviceID: 1, TimestampModified: tm, uuid: randomUUID()}\n" +
                "        CREATE (v2:Interface {Name: 'usfix-rtr1.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr1.federos.com', ZoneID: 1})\n" +
                "SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.1\", uuid: randomUUID()}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm} RETURN COUNT(*) as written;";

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run(federosQuery1);
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("MATCH (tr:TransactionRecord) RETURN tr.transactionData as data");
            //assertEquals(1, result.list().size());
            Record record = result.single();
            Value transactionData = record.get("data");
            System.out.println(transactionData);

            CoreClusterMember leader = targetCluster.awaitLeader();
            GraphDatabaseFacade defaultDB = leader.defaultDatabase();


            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), defaultDB, mock(Log.class));
            graphWriter.executeCRUDOperation();

            org.neo4j.graphdb.Transaction tx = defaultDB.beginTx();
            Iterable<Node> deviceNodes = () -> tx.findNodes(Label.label("Device"));
            Assertions.assertTrue(deviceNodes.iterator().hasNext());
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DNSName")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("TimestampModified")));
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));

            deviceNodes.forEach(node -> assertEquals("usfix-rtr1.federos.com", node.getProperty("Name")));
            deviceNodes.forEach(node -> assertEquals("usfix-rtr1.federos.com", node.getProperty("DNSName")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("DeviceID")));

            deviceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));
            deviceNodes.forEach(node -> assertTrue(node.getSingleRelationship(RelationshipType.withName("HasInterface"), Direction.OUTGOING).hasProperty("uuid")));


            Iterable<Node> interfaceNodes = () -> tx.findNodes(Label.label("Interface"));
            Assertions.assertNotNull(interfaceNodes);
            interfaceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Interface"))));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("CustomName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("IPAddress")));

            interfaceNodes.forEach(node -> assertEquals("usfix-rtr1.federos.com:GigabitEthernet0/0", node.getProperty("Name")));
            interfaceNodes.forEach(node -> assertEquals("usfix-rtr1.federos.com", node.getProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertEquals("GigabitEthernet0/0", node.getProperty("CustomName")));
            interfaceNodes.forEach(node -> assertEquals("192.0.2.1", node.getProperty("IPAddress")));
            interfaceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));


            tx.commit();


        }
    }


    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration4() throws Exception {


        String federosQuery1 = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", uuid: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm} RETURN COUNT(*) as written;";

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run(federosQuery1);
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("MATCH (tr:TransactionRecord) RETURN tr.transactionData as data");
            //assertEquals(1, result.list().size());
            Record record = result.single();
            Value transactionData = record.get("data");
            System.out.println(transactionData);

            CoreClusterMember leader = targetCluster.awaitLeader();
            GraphDatabaseFacade defaultDB = leader.defaultDatabase();


            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), defaultDB, mock(Log.class));
            graphWriter.executeCRUDOperation();

            org.neo4j.graphdb.Transaction tx = defaultDB.beginTx();
            Iterable<Node> deviceNodes = () -> tx.findNodes(Label.label("Device"));
            Assertions.assertTrue(deviceNodes.iterator().hasNext());
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DNSName")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("TimestampModified")));
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));

            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("Name")));
            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DNSName")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("DeviceID")));

            deviceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));
            deviceNodes.forEach(node -> assertTrue(node.getSingleRelationship(RelationshipType.withName("HasInterface"), Direction.OUTGOING).hasProperty("uuid")));


            Iterable<Node> interfaceNodes = () -> tx.findNodes(Label.label("Interface"));
            Assertions.assertNotNull(interfaceNodes);
            interfaceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Interface"))));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("CustomName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("IPAddress")));

            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com:GigabitEthernet0/0", node.getProperty("Name")));
            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertEquals("GigabitEthernet0/0", node.getProperty("CustomName")));
            interfaceNodes.forEach(node -> assertEquals("192.0.2.2", node.getProperty("IPAddress")));
            interfaceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));

            assertTrue(tx.findNodes(Label.label("LocalTx")).hasNext());
            assertFalse(tx.findNodes(Label.label("TransactionRecord")).hasNext());


            tx.commit();


        }
    }

    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration5() throws Exception {

        String federosQuery1 = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", uuid: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm} RETURN COUNT(*) as written;";

        String replicationPollingQuery = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated > %d " +
                "RETURN tr.transactionData as data, tr.timeCreated as time";

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseFacade defaultDB = leader.defaultDatabase();

        long lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(defaultDB);

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run(federosQuery1);
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run(String.format(replicationPollingQuery, lastTransactionTimestamp));
            Record record = result.single();
            Value transactionData = record.get("data");
            Value transactionTime = record.get("time");
            System.out.println(transactionData);

            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), defaultDB, mock(Log.class));
            graphWriter.executeCRUDOperation();


            TransactionHistoryManager.setLastReplicationTimestamp(defaultDB, transactionTime.asLong());


            org.neo4j.graphdb.Transaction tx = defaultDB.beginTx();
            Iterable<Node> deviceNodes = () -> tx.findNodes(Label.label("Device"));
            Assertions.assertTrue(deviceNodes.iterator().hasNext());
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DNSName")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("TimestampModified")));
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));

            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("Name")));
            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DNSName")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("DeviceID")));

            deviceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));
            deviceNodes.forEach(node -> assertTrue(node.getSingleRelationship(RelationshipType.withName("HasInterface"), Direction.OUTGOING).hasProperty("uuid")));


            Iterable<Node> interfaceNodes = () -> tx.findNodes(Label.label("Interface"));
            Assertions.assertNotNull(interfaceNodes);
            interfaceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Interface"))));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("CustomName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("IPAddress")));

            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com:GigabitEthernet0/0", node.getProperty("Name")));
            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertEquals("GigabitEthernet0/0", node.getProperty("CustomName")));
            interfaceNodes.forEach(node -> assertEquals("192.0.2.2", node.getProperty("IPAddress")));
            interfaceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));

            Iterable<Node> lastTransactionReplicatedNodes = () -> tx.findNodes(Label.label("LastTransactionReplicated"));
            Assertions.assertNotNull(lastTransactionReplicatedNodes);
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("LastTransactionReplicated"))));
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasProperty("id")));
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasProperty("lastTimeRecorded")));
            lastTransactionReplicatedNodes.forEach(node -> assertEquals("SINGLETON", node.getProperty("id")));
            lastTransactionReplicatedNodes.forEach(node -> assertEquals(transactionTime.asLong(), node.getProperty("lastTimeRecorded")));


            tx.commit();
        }
    }

    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration6() throws Exception {

        String federosQuery1 = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", uuid: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm} RETURN COUNT(*) as written;";

        String federosQuery2 =
                "MATCH (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1}) " +
                        "MATCH (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1}) " +
                        "MATCH (v1)-[e:HasInterface]->(v2) " +
                        "RETURN COUNT(*) as read;";

        String federosQuery3 =
                "MATCH (v1:Device) RETURN COUNT(v1) as read";

        String replicationPollingQuery = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated > %d " +
                "RETURN tr.transactionData as data, tr.timeCreated as time";

        String transactionRecordQuery = "MATCH (tr:TransactionRecord) " +
                "RETURN count(tr) as txRecords";

        CoreClusterMember leader = targetCluster.awaitLeader();
        GraphDatabaseService defaultDB = leader.managementService().database(DEFAULT_DATABASE_NAME);


        // grab the timestamp from the last transaction replicated to this cluster.

        long lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(defaultDB);

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());

            // run the query provided by Federos that creates a common pattern in their application
            Result result = session.run(federosQuery1);
            // make sure it succeeded.
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            // run the query that polls the source server for transactions that have a timestamp
            // greater than the timestamp of the last replicated transaction.
            // in this case we only have one.  In reality there could be many.
            Result result = session.run(String.format(replicationPollingQuery, lastTransactionTimestamp));
            Record record = result.single();

            // grab the transaction JSON data from the TransactionRecord node
            Value transactionData = record.get("data");
            // grab the timestamp from the TransactionRecord node
            Value transactionTime = record.get("time");
            System.out.println(transactionData);
            System.out.println(transactionTime);


            // create a new GraphWriter instance.  This object knows how to breakdown a transaction message
            // recorded as a JSON String, order the events and write them to the local database.

            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), defaultDB, mock(Log.class));
            graphWriter.executeCRUDOperation();


            // now that we've replicated the transaction from the source
            // let's record the timestamp from the TransactionRecord node locally
            // so we know when the last replica was written.
            // we can then use that timestamp to poll for changes that happen after that timestamp.
            TransactionHistoryManager.setLastReplicationTimestamp(defaultDB, transactionTime.asLong());

            // ensure that what the GraphWriter has written to the local database
            // mirrors what was written at the source database
            org.neo4j.graphdb.Transaction tx = defaultDB.beginTx();
            Iterable<Node> deviceNodes = () -> tx.findNodes(Label.label("Device"));
            Assertions.assertTrue(deviceNodes.iterator().hasNext());
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DNSName")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceID")));
            deviceNodes.forEach(node -> assertTrue(node.hasProperty("TimestampModified")));
            deviceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Device"))));

            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("Name")));
            deviceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DNSName")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            deviceNodes.forEach(node -> assertEquals(1, node.getProperty("DeviceID")));

            deviceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));
            deviceNodes.forEach(node -> assertTrue(node.getSingleRelationship(RelationshipType.withName("HasInterface"), Direction.OUTGOING).hasProperty("uuid")));


            Iterable<Node> interfaceNodes = () -> tx.findNodes(Label.label("Interface"));
            Assertions.assertTrue(interfaceNodes.iterator().hasNext());
            interfaceNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("Interface"))));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("uuid")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("Name")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("CustomName")));
            interfaceNodes.forEach(node -> assertTrue(node.hasProperty("IPAddress")));

            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com:GigabitEthernet0/0", node.getProperty("Name")));
            interfaceNodes.forEach(node -> assertEquals("usfix-rtr2.federos.com", node.getProperty("DeviceName")));
            interfaceNodes.forEach(node -> assertEquals(1, node.getProperty("ZoneID")));
            interfaceNodes.forEach(node -> assertEquals("GigabitEthernet0/0", node.getProperty("CustomName")));
            interfaceNodes.forEach(node -> assertEquals("192.0.2.2", node.getProperty("IPAddress")));
            interfaceNodes.forEach(node -> assertTrue(node.hasRelationship(RelationshipType.withName("HasInterface"))));
            System.out.println(interfaceNodes.iterator().next().getProperty("uuid"));

            // make sure we have our LastTransactionReplicated node.
            // this node is where we store the timestamp for the last replica written locally.

            Iterable<Node> lastTransactionReplicatedNodes = () -> tx.findNodes(Label.label("LastTransactionReplicated"));
            Assertions.assertNotNull(lastTransactionReplicatedNodes);
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasLabel(Label.label("LastTransactionReplicated"))));
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasProperty("id")));
            lastTransactionReplicatedNodes.forEach(node -> assertTrue(node.hasProperty("lastTimeRecorded")));
            lastTransactionReplicatedNodes.forEach(node -> assertEquals("SINGLETON", node.getProperty("id")));
            lastTransactionReplicatedNodes.forEach(node -> assertEquals(transactionTime.asLong(), node.getProperty("lastTimeRecorded")));

//            org.neo4j.graphdb.Result queryResult = tx.execute(federosQuery2);
//            assertTrue(queryResult.hasNext());
//            assertEquals(1,queryResult.next().get("read"));


            // just for readability, we run a match statement that should succeed
            // on both source and target clusters.

//            org.neo4j.graphdb.Result queryResult = tx.execute(federosQuery3);
//            assertTrue(queryResult.hasNext());
//            assertEquals(1,queryResult.next().get("read"));

            // here we make sure that the transaction we replicated
            // won't generate it's own TransactionRecord node
            // and be replicated back to the source.
            // If there were one created for either the replicated transaction
            // or the LastTransactionReplicated node then we would create
            // an infinite loop of replicated transactions.

            org.neo4j.graphdb.Result queryResult2 = tx.execute(transactionRecordQuery);
            assertTrue(queryResult2.hasNext());
            assertEquals(0, (long) queryResult2.next().get("txRecords"));

            // hopefully we only see green checks!

            tx.commit();


            System.out.println("Sanity check!  We've made it through the test");


        }
    }

    @Test
    public void runningSameQueryOnSourceAndTargetTest() throws Exception {

        String sourceWriteQuery = "WITH timestamp() AS tm\n" +
                "        MERGE (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET v1 += {DNSName: \"usfix-rtr2.federos.com\", DeviceID: 1, TimestampModified: tm}\n" +
                "        MERGE (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1})\n" +
                "ON CREATE SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\", uuid: randomUUID()}\n" +
                "ON MATCH SET v2 += {CustomName: \"GigabitEthernet0/0\", IPAddress: \"192.0.2.2\"}\n" +
                "        MERGE (v1)-[e:HasInterface]->(v2)\n" +
                "ON CREATE SET e += {TimestampModified: tm, uuid: randomUUID()}\n" +
                "ON MATCH SET e += {TimestampModified: tm} RETURN COUNT(*) as written;";

        String queryThatShouldReturnAPathOnBoth =
                "MATCH (v1:Device {Name: 'usfix-rtr2.federos.com', ZoneID: 1}) " +
                        "MATCH (v2:Interface {Name: 'usfix-rtr2.federos.com:GigabitEthernet0/0', DeviceName: 'usfix-rtr2.federos.com', ZoneID: 1}) " +
                        "MATCH (v1)-[e:HasInterface]->(v2) " +
                        "RETURN COUNT(*) as read;";

        String replicationPollingQuery = "MATCH (tr:TransactionRecord) WHERE tr.timeCreated > %d " +
                "RETURN tr.transactionData as data, tr.timeCreated as time";


        CoreClusterMember targetLeader = targetCluster.awaitLeader();
        GraphDatabaseFacade targetDefaultDB = targetLeader.defaultDatabase();

        CoreClusterMember sourceLeader = sourceCluster.awaitLeader();
        GraphDatabaseFacade sourceDefaultDB = sourceLeader.defaultDatabase();

        // grab the timestamp from the last transaction replicated to this cluster.

        long lastTransactionTimestamp = TransactionHistoryManager.getLastReplicationTimestamp(targetDefaultDB);

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());

            // run the query provided by Federos that creates a common pattern in their application
            Result result = session.run(sourceWriteQuery);
            // make sure it succeeded.
            assertEquals(1, result.list().size());

        }

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {

            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            // run the query that polls the source server for transactions that have a timestamp
            // greater than the timestamp of the last replicated transaction.
            // in this case we only have one.  In reality there could be many.
            Result result = session.run(String.format(replicationPollingQuery, lastTransactionTimestamp));
            Record record = result.single();

            // grab the transaction JSON data from the TransactionRecord node
            Value transactionData = record.get("data");
            // grab the timestamp from the TransactionRecord node
            Value transactionTime = record.get("time");
            System.out.println(transactionData);

            // create a new GraphWriter instance.  This object knows how to breakdown a transaction message
            // recorded as a JSON String, order the events and write them to the local database.

            GraphWriter graphWriter = new GraphWriter(transactionData.asString(), targetDefaultDB, mock(Log.class));
            graphWriter.executeCRUDOperation();


            // now that we've replicated the transaction from the source
            // let's record the timestamp from the TransactionRecord node locally
            // so we know when the last replica was written.
            // we can then use that timestamp to poll for changes that happen after that timestamp.
            TransactionHistoryManager.setLastReplicationTimestamp(targetDefaultDB, transactionTime.asLong());


            // just for readability, we run a match statement that should succeed
            // on both source and target clusters.

            // query on target
            org.neo4j.graphdb.Transaction targetTx = targetDefaultDB.beginTx();
            org.neo4j.graphdb.Result targetQueryResult = targetTx.execute(queryThatShouldReturnAPathOnBoth);
            assertTrue(targetQueryResult.hasNext());
            assertEquals(1, (long) targetQueryResult.next().get("read"));
            targetTx.commit();

            // same query at source
            org.neo4j.graphdb.Transaction sourceTx = sourceDefaultDB.beginTx();
            org.neo4j.graphdb.Result sourceQueryResult = sourceTx.execute(queryThatShouldReturnAPathOnBoth);
            assertTrue(sourceQueryResult.hasNext());
            assertEquals(1, (long) sourceQueryResult.next().get("read"));
            sourceTx.commit();


            System.out.println("Sanity check!  We've made it through the test");


        }
    }

    private class DatabaseIsReadyListener extends DatabaseEventListenerAdapter {
        private boolean ready = false;

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {
            this.ready = true;
        }

        public DatabaseIsReadyListener() {
            super();

        }

        public void waitUntilReady() {
            while (!ready) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
