package com.neo4j.sync.procedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.UUID;

public class StatementReplicationProcedures {


    private final static String TX_RECORD_LABEL = "TransactionRecord";
    private final static String TX_RECORD_NODE_BEFORE_COMMIT_KEY = "transactionUUID";
    private final static String TX_RECORD_STATUS_KEY = "status";
    private final static String TX_RECORD_TX_DATA_KEY = "transactionData";
    private final static String TX_RECORD_CREATE_TIME_KEY = "timeCreated";
    private final static String ST_TX_RECORD_LABEL = "StatementRecord";
    private final static String ST_TX_RECORD_TX_DATA_KEY = "transactionStatement";
    private final static String ST_DATA_JSON = "{\"statement\":\"true\"}";

    @Context
    public Log log;
    @Context
    public GraphDatabaseService gds;

    @Procedure(name = "replicateStatement", mode = Mode.WRITE)
    @Description("commits the statement and creates a StatementRecord for replication.")
    public void replicateStatement(@Name(value = "statement") String statement) {

        try (Transaction tx = gds.beginTx()) {
            tx.execute(statement);
            tx.commit();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

        try (Transaction tx = gds.beginTx()) {
            Node txRecordNode = tx.createNode(Label.label(TX_RECORD_LABEL));
            txRecordNode.addLabel(Label.label(ST_TX_RECORD_LABEL));
            txRecordNode.setProperty(TX_RECORD_STATUS_KEY, "AFTER_COMMIT");
            txRecordNode.setProperty(TX_RECORD_CREATE_TIME_KEY, System.currentTimeMillis());
            txRecordNode.setProperty(TX_RECORD_NODE_BEFORE_COMMIT_KEY, UUID.randomUUID().toString());
            txRecordNode.setProperty(ST_TX_RECORD_TX_DATA_KEY, statement);
            txRecordNode.setProperty(TX_RECORD_TX_DATA_KEY, ST_DATA_JSON);
            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
