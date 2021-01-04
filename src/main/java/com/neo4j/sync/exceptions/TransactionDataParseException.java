package com.neo4j.sync.exceptions;

public class TransactionDataParseException extends ReplicationException {
    public TransactionDataParseException(String message) {
        super(message);
    }
}
