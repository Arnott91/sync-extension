package com.neo4j.sync.engine;

/**
 * com.neo4j.sync.engine.NodeDirection enum provides an enumeration of node orientation with respect
 * to relationships.  It's used in the com.neo4j.sync.engine.GraphWriter and TransactionDataHandler classes
 *
 * @author Chris Upkes
 */

public enum NodeDirection {

    START, TARGET
}
