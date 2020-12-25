package com.neo4j.sync.exceptions;



public class TransactionRecordingException extends ReplicationException {

    public TransactionRecordingException(String message) {
        super(message);
    }
}
