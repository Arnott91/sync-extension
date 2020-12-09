import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
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

@TestInstance( TestInstance.Lifecycle.PER_METHOD )
@ClusterExtension
public class MultiDbTest {

    public static final String INTEGRATION_DB_NAME =  format("Integrationdb-%s", UUID.randomUUID());

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers( 3 )
            .withSharedCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3" )
            .withNumberOfReadReplicas( 0 );

    @BeforeEach
    void setup() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldCreateMultipleDatabasesPerServer() throws Exception
    {
        // Given
        FederosTransactionEventListenerAdapter listener = new FederosTransactionEventListenerAdapter();
        for (CoreClusterMember coreMember : cluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        cluster.awaitLeader().managementService().createDatabase(INTEGRATION_DB_NAME);

    }
}
