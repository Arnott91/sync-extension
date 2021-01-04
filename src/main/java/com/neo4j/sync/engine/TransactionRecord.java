package com.neo4j.sync.engine;


/**
 * com.neo4j.sync.engine.TransactionRecord contains a collection of audit objects that reflect all of the changes resulting from
 * a single transaction.  I created this class with the assumption that the system may want to
 * serialize the transaction data in numerous ways.  This class is where a calling system would define
 * API requirements for retrieving the transaction data in another format.
 * i.e. getJSONTransactionData...getXMLTransactionData...at this writing we only support JSON.
 *
 * @author Chris Upkes
 */

public class TransactionRecord {
    private final long timestampCreated;
    private final String status;
    private final String transactionData;
    private final String transactionUUID;
    public TransactionRecord(long timestampCreated, String status, String transactionData, String transactionUUID) {

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

    public long getTimestampCreated() {
        return timestampCreated;
    }

    public String getTransactionUUID() {
        return transactionUUID;
    }

    private String wrapTransactionMessage(String transactionData) {
        // the JSON from the is incomplete..
        // wrap the JSON array in a valid JSON root
        return "{\"transactionEvents\":" + transactionData + "}";

    }


}



