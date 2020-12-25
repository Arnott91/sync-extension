package com.neo4j.sync.engine;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.List;
import java.util.Map;


public class TransactionDataHandler {

    private final List<Map<String, JSONObject>> transactionEvents;
    private Transaction tx;
    private Log log;


    public TransactionDataHandler(String transactionData, Transaction tx) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(transactionData));
        this.tx = tx;
    }

    public TransactionDataHandler(String transactionData, List<Map<String, JSONObject>> transactionEvents, Transaction tx, Log log) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(new JSONObject(transactionData));
        this.log = log;
        this.tx = tx;
    }

    public TransactionDataHandler(JSONObject transactionData, Transaction tx) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(transactionData);
        this.tx = tx;
    }

    public TransactionDataHandler(JSONObject transactionData, Transaction tx, Log log) throws JSONException {
        this.transactionEvents = TransactionDataParser.getTransactionEvents(transactionData);
        this.log = log;
        this.tx = tx;
    }

}
