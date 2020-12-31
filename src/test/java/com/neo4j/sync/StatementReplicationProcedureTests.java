package com.neo4j.sync;

import com.neo4j.sync.procedures.StatementReplicationProcedures;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.*;
import org.neo4j.fabric.stream.StatementResult;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.harness.junit.rule.Neo4jRule;


import javax.ws.rs.core.Context;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.Values.parameters;

public class StatementReplicationProcedureTests
{
    // This rule starts a Neo4j instance

//    @Context
//    private GraphDatabase gds;
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()

            // This is the Procedure to test
            .withProcedure( StatementReplicationProcedures.class );

    @Test
    public void shouldAllowIndexingAndFindingANode() throws Throwable
    {
        // In a try-block, to make sure you close the driver after the test
        try( Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.builder().withoutEncryption().build()) )
        {

            // Given I've started Neo4j with the FullTextIndex procedure class
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // And given I have a node in the database
            session.run( "CALL replicateStatement('CREATE INDEX FOR (p:Person) ON (p.name)')" );

            assertEquals(1,session.run( "MATCH (sr:StatementRecord) RETURN count(sr) AS records" )
                    .single()
                    .get( 0 ).asLong());

            assertEquals("CREATE INDEX FOR (p:Person) ON (p.name)",session.run( "MATCH (sr:StatementRecord) RETURN sr.transactionStatement" )
                    .single()
                    .get( 0 ).asString());

            ;
        }
    }
}
