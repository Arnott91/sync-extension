package com.neo4j.sync.engine;

import org.neo4j.graphdb.Node;

/**
 * com.neo4j.sync.engine.TransactionRecord contains a collection of audit objects that reflect all of the changes resulting from
 * a single transaction.
 *
 * @author Chris Upkes
 */

public class TransactionRecord {
    public TransactionRecord(String timestampCreated, String status, String transactionData, String transactionUUID)
    {

        this.timestampCreated = timestampCreated;
        this.status = status;
        this.transactionData = transactionData;
        this.transactionUUID = transactionUUID;
    }


    private String timestampCreated;
    private String status;
    private String transactionData;
    private String transactionUUID;


    public String getTransactionData() {
        return transactionData;
    }

    public String getStatus() {
        return status;
    }

    public String getTimestampCreated() {
        return timestampCreated;
    }


    public String getTransactionUUID() {
        return transactionUUID;
    }


}



