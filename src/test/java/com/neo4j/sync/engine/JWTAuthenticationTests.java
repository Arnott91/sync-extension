package com.neo4j.sync.engine;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.sync.listener.CaptureTransactionEventListenerAdapter;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import io.jsonwebtoken.*;
import io.jsonwebtoken.impl.crypto.MacProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.driver.*;
import org.neo4j.test.extension.Inject;

import java.net.URI;
import java.security.Key;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.driver.GraphDatabase.driver;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@ClusterExtension
public class JWTAuthenticationTests {


    private static final java.util.UUID UUID = null;
    private final ClusterConfig clusterConfig = ClusterConfig
            .clusterConfig()
            .withNumberOfCoreMembers(3)
            .withSharedCoreParam(CausalClusteringSettings.minimum_core_cluster_size_at_formation, "3")
            .withNumberOfReadReplicas(0);
    @Inject
    // the clusterFactory is initialized because of the @ClusterExtension annotation (pre-compiler stuff).
    private ClusterFactory clusterFactory;
    private Cluster sourceCluster;
    private CaptureTransactionEventListenerAdapter listener;

    @BeforeEach
    void setup() throws Exception {
        sourceCluster = clusterFactory.createCluster(clusterConfig);
        sourceCluster.start();
        // grab an instance of our TransactionEvent listener.
        listener = new CaptureTransactionEventListenerAdapter();
        // register the listener with the cluster.
        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }


    }

    @AfterEach
    void cleanUp() {
        // unregister the listener.  Best practice housecleaning.
        for (CoreClusterMember coreMember : sourceCluster.coreMembers()) {
            coreMember.managementService().unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }


    @Test
    public void listenerShouldListenToDefaultAndWriteToIntegration() throws Exception {
        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {
            // create a pattern on the source cluster.
            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("CREATE (p:Person {uuid:'Rosa'})-[:FOLLOWS]->(:Person {uuid:'Karl'}) RETURN p");
            // ascertain success
            assertEquals(1, result.list().size());

        }
        // the TransactionEventHandlerAdapter will catch the transaction and record transaction details
        // in a TransactionRecord node that is stored on that cluster

        try (Driver driver = driver(new URI("bolt://" + sourceCluster.awaitLeader().boltAdvertisedAddress()), AuthTokens.basic("neo4j", "password"))) {
            // grab the newly minted TransactionRecord node from the source cluster.
            Session session = driver.session(SessionConfig.builder().withDatabase(DEFAULT_DATABASE_NAME).build());
            Result result = session.run("MATCH (tr:TransactionRecord) RETURN tr");
            // there can only be one!
            assertEquals(1, result.list().size());
        }
    }

    @Test
    public void simpleJWTUseTest() {

        Key key = MacProvider.generateKey();

        String jwtString = Jwts.builder().setSubject("Joe").signWith(SignatureAlgorithm.HS512, key).compact();
        assert Jwts.parser().setSigningKey(key).parseClaimsJws(jwtString).getBody().getSubject().equals("Joe");
    }

    @Test
    public void simpleJWTSBuilderTest() throws Exception {

        Instant thisInstant = Instant.now();
        String jwtToken = Jwts.builder()
                .claim("name", "Jane Doe")
                .claim("email", "jane@example.com")
                .setSubject("jane")
                .setId(java.util.UUID.randomUUID().toString())
                .setIssuedAt(Date.from(thisInstant))
                .setExpiration(Date.from(thisInstant.plus(5l, ChronoUnit.MINUTES)))
                .compact();

        System.out.println(jwtToken);

        Jwt<Header, Claims> jwt = Jwts.parserBuilder()
                .setSigningKey("x")
                .build()
                .parseClaimsJwt(jwtToken);

        assertTrue(jwt.getHeader().size() > 0);
        System.out.println(jwt.getHeader().getType());
        System.out.println(jwt.getBody().getId());
        assertEquals(jwt.getBody().getSubject(), "jane");
    }
}
