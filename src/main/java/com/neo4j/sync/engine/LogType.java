package com.neo4j.sync.engine;

/**
 * com.neo4j.sync.engine.LogType enum provides an enumeration of log types for use with
 * .the com.neo4j.sync.engine.TransactionFileLogger
 *
 * @author Chris Upkes
 */

public enum LogType {

    OUTBOUND_TX, INBOUND_TX, TX_POLLING, DEBUG
}
