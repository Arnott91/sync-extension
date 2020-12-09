import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.graphdb.Node;
import org.neo4j.test.extension.Inject;

import java.net.URI;
import java.util.UUID;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.Values.parameters;
import static org.neo4j.graphdb.Label.label;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class PullFromRemoteClusterIT {

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
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, new FederosTransactionEventListenerAdapter());
        }

        // Don't need the listener on the sink cluster for this test

        sourceCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DB_NAME);
        sourceCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DB_NAME);
        sinkCluster.awaitLeader().managementService().createDatabase(INTEGRATION_DB_NAME);
        sinkCluster.awaitLeader().managementService().startDatabase(INTEGRATION_DB_NAME);

        // Create some data
        sourceCluster.coreTx(INTEGRATION_DB_NAME,  ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } );
    }

    @Test
    public void shouldPullFromRemoteDB() throws Exception {

        Driver driver = GraphDatabase.driver(new URI("neo4j://"+sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"));

        try (Session session = driver.session(SessionConfig.builder().withDatabase(INTEGRATION_DB_NAME).build())) {
            Result result = session.run(("MATCH (n) RETURN (n)"));

            while (result.hasNext())
            {
                Record record = result.next();
                // Values can be extracted from a record by index or name.
                System.out.println(record);
            }
        }

        driver.close();
    }
}
