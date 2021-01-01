package com.neo4j.sync.engine;

/**
 * com.neo4j.sync.engine.ParseType enum provides an enumeration of JSON data parsing types for use with
 * the com.neo4j.sync.engine.TransactionDataParser.
 *
 * @author Chris Upkes
 */

public enum ParseType {

    PRIMARY_KEY, NODE_PROPERTIES, REL_PROPERTIES

}
