package com.neo4j.sync;

import com.neo4j.sync.procedures.StatementReplicationProcedures;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.harness.junit.rule.Neo4jRule;

import static org.junit.Assert.assertEquals;

public class StatementReplicationProcedureTests {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule().withProcedure(StatementReplicationProcedures.class);

    @Test
    public void shouldAllowIndexingAndFindingANode() {
        // In a try-block, to make sure you close the driver after the test
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.builder().withoutEncryption().build())) {

            // Given I've started Neo4j with the FullTextIndex procedure class
            //       which my 'neo4j' rule above does.
            Session session = driver.session();

            // And given I have a node in the database
            session.run("CALL replicateStatement('CREATE INDEX FOR (p:Person) ON (p.name)')");

            assertEquals(1, session.run("MATCH (sr:StatementRecord) RETURN count(sr) AS records")
                    .single()
                    .get(0).asLong());

            assertEquals("CREATE INDEX FOR (p:Person) ON (p.name)", session.run("MATCH (sr:StatementRecord) RETURN sr.transactionStatement")
                    .single()
                    .get(0).asString());
        }
    }
}
