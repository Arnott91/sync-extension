package com.neo4j.sync.engine;


/**
 * com.neo4j.sync.engine.TransactionRecord contains a collection of audit objects that reflect all of the changes resulting from
 * a single transaction.
 *
 * @author Chris Upkes
 */

public class TransactionRecord {
    private final String timestampCreated;
    private final String status;
    private final String transactionData;
    private final String transactionUUID;
    public TransactionRecord(String timestampCreated, String status, String transactionData, String transactionUUID) {

        this.timestampCreated = timestampCreated;
        this.status = status;
        this.transactionData = this.wrapTransactionMessage(transactionData);
        this.transactionUUID = transactionUUID;
    }

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

    private String wrapTransactionMessage(String transactionData) {
        // remove the surrounding brackets of each transaction message.
        return "{\"transactionEvents\":" + transactionData + "}";

    }


}



