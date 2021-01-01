package com.neo4j.sync.engine;

import org.neo4j.graphdb.Node;

/**
 * com.neo4j.sync.engine.AuditNode contains information about changes made in any transaction. It also contains metadata about who
 * made the change and when.
 *
 * @author Ravi Anthapu, Chris Upkes
 */

class AuditNode {
    static final String ADD_NODE = "AddNode";
    static final String DELETE_NODE = "DeleteNode";
    static final String ADD_RELATION = "AddRelation";
    static final String DELETE_RELATION = "DeleteRelation";
    static final String NODE_PROPERTY_CHANGE = "NodePropertyChange";
    static final String RELATION_PROPERTY_CHANGE = "RelationPropertyChange";

    private Node node;

    private Audit audit;

    public AuditNode() {
        audit = new Audit();
    }

    public Audit getAudit() {
        return audit;
    }

    public void setAudit(Audit audit) {
        this.audit = audit;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}