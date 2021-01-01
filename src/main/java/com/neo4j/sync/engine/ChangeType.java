package com.neo4j.sync.engine;

/**
 * com.neo4j.sync.engine.ChangeType enum provides an enumeration of transaction events.
 *
 * @author Chris Upkes
 */

public enum ChangeType {

    ADD_NODE, DELETE_NODE, ADD_RELATION, DELETE_RELATION, NODE_PROPERTY_CHANGE, RELATION_PROPERTY_CHANGE
}
