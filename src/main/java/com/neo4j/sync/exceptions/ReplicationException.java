package com.neo4j.sync.exceptions;

import org.neo4j.graphdb.TransactionFailureException;

public class ReplicationException extends Exception {

    public ReplicationException(String message) {
        super(message);
    }
}

