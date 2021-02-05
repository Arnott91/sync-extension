package com.neo4j.sync.procedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.UUID;

/**
 * Protocol is as follows.
 * <p>
 * the statement replication procedure accepts a single cypher statement and uses the internal API
 * to execute the statement.  After successful execution of the statement, the procedure then
 * stores the executed statement on StatementRecord node that is a special kind of
 * TransactionRecord node that provides replication data as a statement instead of a JSON string.
 * </p>
 *
 * @author Chris Upkes
 */
public class StatementReplicationProcedures {

    @Context
    public Log log;

    @Context
    public GraphDatabaseService gds;

    private static final String TX_RECORD_LABEL = "TransactionRecord";
    private static final String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private static final String TX_RECORD_STATUS_KEY = "status";
    private static final String TX_RECORD_TX_DATA_KEY = "transactionData";
    private static final String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private static final String ST_TX_RECORD_LABEL = "StatementRecord";
    private static final String ST_TX_RECORD_TX_DATA_KEY = "transactionStatement";
    private static final String ST_DATA_JSON = "{\"statement\":\"true\"}";

    @Procedure(name = "replicateStatement", mode = Mode.WRITE)
    @Description("Commits the statement and creates a StatementRecord for replication.")
    public void replicateStatement(@Name(value = "statement") String statement) {
        // we we simply execute the statement passed into the procedure
        try (Transaction tx = gds.beginTx()) {
            tx.execute(statement);
            tx.commit();
            log.debug("Replicating statement: " + statement);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        /* Here we create a special TransactionRecord:StatementRecord node
        that records the statement string in a property */
        try (Transaction tx = gds.beginTx()) {
            Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
            txRecordNode.addLabel(Label.label(ST_TX_RECORD_LABEL));
            txRecordNode.setProperty(TX_RECORD_STATUS_KEY, "AFTER_COMMIT");
            txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, System.currentTimeMillis());
            txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, UUID.randomUUID().toString());
            txRecordNode.setProperty(ST_TX_RECORD_TX_DATA_KEY, statement);
            txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, ST_DATA_JSON);
            tx.commit();
            log.debug("StatementRecord written for statement: " + statement);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
