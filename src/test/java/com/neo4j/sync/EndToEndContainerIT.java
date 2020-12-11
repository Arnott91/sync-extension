package com.neo4j.sync;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.procedures.StartAndStopReplicationProcedures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.shaded.com.google.common.io.Files;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.junit.jupiter.causal_cluster.CausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.CoreModifier;
import org.neo4j.junit.jupiter.causal_cluster.NeedsCausalCluster;
import org.neo4j.junit.jupiter.causal_cluster.Neo4jCluster;
import org.neo4j.test.jar.JarBuilder;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
@NeedsCausalCluster( neo4jVersion = "4.2" )
public class EndToEndContainerIT
{
    private static final String myPlugin = "myPlugin.jar";

    private static final AuthToken authToken = AuthTokens.basic( "neo4j", "password" );

    public static final String INTEGRATION_DB_NAME = format( "Integrationdb-%s", UUID.randomUUID() );

    @CausalCluster
    private static Neo4jCluster cluster;

    @CoreModifier
    private static Neo4jContainer<?> configure( Neo4jContainer<?> input ) throws IOException
    {
        var pluginsDir = Files.createTempDir();
        var myPluginJar = pluginsDir.toPath().resolve( myPlugin );

        new JarBuilder().createJarFor( myPluginJar, StartAndStopReplicationProcedures.class, ReplicationEngine.class );

        return input.withNeo4jConfig( "dbms.security.procedures.unrestricted", "*" )
                    .withCopyFileToContainer( MountableFile.forHostPath( myPluginJar ), "/plugins/myPlugin.jar" );
    }

    @BeforeEach
    void setup() throws Exception
    {

    }

    @Test
    public void shouldPullFromRemoteDatabaseAndStoreInLocalDatabase() throws Exception
    {

        try ( Driver coreDriver = GraphDatabase.driver( cluster.getURI(), authToken ) )
        {
            Session session = coreDriver.session();
            Result res = session.run( "CALL dbms.procedures() YIELD name, signature RETURN name, signature" );

            // Then the procedure from the plugin is listed
            assertTrue( res.stream().anyMatch( x -> x.get( "name" ).asString().contains( "startReplication" ) ),
                        "Missing procedure provided by our plugin" );
        }

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
