package com.neo4j.sync.engine;

import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.logging.Log;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/***
 * The transaction recorder is used to capture and serialize transaction information maintained
 * by the Neo4j server as part of the transaction handling mechanism.
 * @author Ravi Anthapu
 * @author Chris Upkes
 */

@SuppressWarnings("DuplicatedCode")
public class TransactionRecorder {

    public static final String UUID = "uuid";
    private final TransactionData transactionData;
    private final Log log;

    public TransactionRecorder(TransactionData txData, Log log) {
        this.transactionData = txData;
        this.log = log;
    }

    // This method works on the transaction data object passed to the constructor
    // and returns a new transaction record object that can be used
    // to write transaction history as a node locally or to a file
    // or another system.
    public TransactionRecord serializeTransaction() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        HashMap<Node, Map<String, AuditNode>> auditValues = new HashMap<>();


        for (Node node : transactionData.createdNodes())
        {
            Iterable<Label> labels = node.getLabels();
            for (Label l : labels)
            {
                if (l.name().equalsIgnoreCase("DONT_AUDIT"))
                {
                    // Delete the DONT_AUDIT node so that it's not persisted to the database
                    node.delete();
                    return null;
                }

            }
        }

        for (Node createdNode : transactionData.createdNodes()) {
            processAddRemoveNode(createdNode, auditValues, AuditNode.ADD_NODE,
                    transactionData);
        }

        for (Node deletedNode : transactionData.deletedNodes()) {
            processAddRemoveNode(deletedNode, auditValues, AuditNode.DELETE_NODE, transactionData);
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
            processNodePropertyChange(propertyEntry, auditValues);
        }

        for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
            processNodePropertyChange(propertyEntry, auditValues);
        }

        for (Relationship createdRelationship : transactionData.createdRelationships()) {
            processAddRemoveRelationship(createdRelationship, auditValues, AuditNode.ADD_RELATION);
        }

        for (Relationship deletedRelationship : transactionData.deletedRelationships()) {
            processAddRemoveRelationship(deletedRelationship, auditValues, AuditNode.DELETE_RELATION) ;
        }

        for (PropertyEntry<Relationship> propertyEntry : transactionData.assignedRelationshipProperties()) {
            processRelationPropertyChange(propertyEntry, auditValues
            );
        }
        for (PropertyEntry<Relationship> propertyEntry : transactionData.removedRelationshipProperties()) {
            processRelationPropertyChange(propertyEntry, auditValues
            );
        }

        // Generates audit
        List<AuditNode> nodesToProcess = new ArrayList<>();
        String transactionUUID = java.util.UUID.randomUUID().toString();
        //Long transactionId = transactionData.getTransactionId();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        //String timestampCreated = timestamp.toString();
        long timestampCreated = timestamp.getTime();



        for (Map<String, AuditNode> auditProps : auditValues.values())
        {
            for (AuditNode auditNode : auditProps.values())
            {
                if (auditNode.getAudit().getPrimaryKey() == null)
                {
                    /*
                    This is most likely a deleted node or Add node.
                    Let's check the primary key can be obtained for this node. If not we need to ignore the node.
                     */
                    if (getDeletedNodePrimaryKey(auditNode) == null)
                    {
                        continue;
                    }
                }

                auditNode.setNode(null);
                nodesToProcess.add(auditNode);
            }
        }


        List<Audit> audits = new ArrayList<>();
        for (AuditNode auditNode : nodesToProcess)
        {
            audits.add(auditNode.getAudit());
        }
        String data = objectMapper.writeValueAsString(audits);
        log.debug("TransactionRecorder -> transaction data: ", data);

        return  new TransactionRecord(timestampCreated, "NEW",  objectMapper.writeValueAsString(audits), transactionUUID);

    }



    // This method is used to create audit records for added and deleted nodes
    private void processAddRemoveNode(Node node, Map<Node, Map<String, AuditNode>> auditValues,
                                      String changeType, TransactionData transactionData)
    {
        Map<String, AuditNode> nodeChanges = auditValues.computeIfAbsent(node,
                k -> new HashMap<>());

        AuditNode auditNode = nodeChanges.get(changeType);
        if (auditNode == null)
        {
            auditNode = new AuditNode();
            auditNode.getAudit().setChangeType(changeType);
            auditNode.setNode(node);

            nodeChanges.put(changeType, auditNode);

            List<String> nodeLabels = new ArrayList<>();

            if (changeType.equalsIgnoreCase(AuditNode.ADD_NODE)) {
                node.getLabels().forEach(entry -> nodeLabels.add(entry.name()));

                auditNode.getAudit().setNodeLabels(nodeLabels);

                auditNode.getAudit().setPrimaryKey(getNodePrimaryKey(node));
                auditNode.getAudit().setAllProperties(node.getAllProperties());
            }
            else
            {
                for (LabelEntry labelEntry : transactionData.removedLabels())
                {
                    if (labelEntry.node().getId() == node.getId())
                    {
                        // We found the Deleted node.
                        String label = labelEntry.label().name();
                        nodeLabels.add(label);
                        auditNode.getAudit().setAllProperties(new HashMap<>());
                    }
                }
                auditNode.getAudit().setNodeLabels(nodeLabels);
            }
        }
    }

    // This method is used to create audit records for add and delete relationship
    private void processAddRemoveRelationship(Relationship relationship,
                                              Map<Node, Map<String, AuditNode>> auditValues, String changeType) {
        Node node = relationship.getStartNode();
        Map<String, AuditNode> nodeChanges = auditValues.get(node);
        AuditNode deletedNode = null;

        if (nodeChanges == null)
        {
            nodeChanges = new HashMap<>();
            auditValues.put(node, nodeChanges);
        }
        else
        {
            // Look if this node has deleted node changes.
            // If so we need to look at the com.neo4j.sync.engine.AuditNode captured
            // for this deleted node to read label and properties.
            deletedNode = nodeChanges.get(AuditNode.DELETE_NODE);
        }

        AuditNode auditNode = nodeChanges.get(getRelationChangeType(changeType, relationship));
        if (auditNode == null)
        {
            auditNode = new AuditNode();
            auditNode.getAudit().setChangeType(changeType);
            auditNode.setNode(node);
            if (deletedNode != null)
            {
                auditNode.getAudit().setNodeLabels(deletedNode.getAudit().getNodeLabels());
                auditNode.getAudit().setPrimaryKey(getDeletedNodePrimaryKey(deletedNode));
            }
            else
            {
                List<String> nodeLabels = new ArrayList<>();
                node.getLabels().forEach(entry -> nodeLabels.add(entry.name()));
                auditNode.getAudit().setNodeLabels(nodeLabels);
                auditNode.getAudit().setPrimaryKey(getNodePrimaryKey(node));
            }
            auditNode.getAudit().setRelationshipLabel(relationship.getType().name());
            if (changeType.equals(AuditNode.ADD_RELATION))
            {
                auditNode.getAudit().setAllProperties(relationship.getAllProperties());
            }
            else
            {
                auditNode.getAudit().setAllProperties(new HashMap<>());
            }

            Node endNode = relationship.getEndNode();
            deletedNode = checkForDeletedNode(endNode, auditValues);
            if (deletedNode != null)
            {
                auditNode.getAudit().setTargetNodeLabels(deletedNode.getAudit().getNodeLabels());
                auditNode.getAudit().setTargetPrimaryKey(getDeletedNodePrimaryKey(deletedNode));
            }
            else
            {
                List<String> nodeLabels = new ArrayList<>();
                endNode.getLabels().forEach(entry -> nodeLabels.add(entry.name()));
                auditNode.getAudit().setTargetNodeLabels(nodeLabels);
                auditNode.getAudit().setTargetPrimaryKey(getNodePrimaryKey(endNode));
            }

            nodeChanges.put(getRelationChangeType(changeType, relationship), auditNode);
        }
    }

    private AuditNode checkForDeletedNode(Node node, Map<Node, Map<String, AuditNode>> auditValues) {
        AuditNode aNode = null;
        Map<String, AuditNode> nodeChanges = auditValues.get(node);
        if (nodeChanges != null)
        {
            aNode = nodeChanges.get(AuditNode.DELETE_NODE);
        }
        return aNode;
    }

    // This method is used to create audit records for node property change
    private void processNodePropertyChange(PropertyEntry<Node> propertyEntry, Map<Node, Map<String, AuditNode>> auditValues) {
        Node node = propertyEntry.entity();

        Map<String, AuditNode> nodeChanges = auditValues.computeIfAbsent(node,
                k -> new HashMap<>());

        AuditNode auditNode;

        auditNode = nodeChanges.get(AuditNode.ADD_NODE);
        if (auditNode != null)
        {
            // There is a new node created for this. This means all the properties
            // that were added to the node using set will be called as node property changes.
            // The all properties would be set in handling the Add node logic.
            return;
        }

        auditNode = nodeChanges.get(AuditNode.DELETE_NODE);
        if (auditNode != null)
        {
            // This node is deleted. This means all the properties
            // that were part of node will come as deleted properties.
            // We need to add them to the list here as node.getAllProperties() cannot be done.

            if (auditNode.getAudit().getNodeLabels() != null)
            {
                // We will populate properties only if the node label is set. This won't be set for
                // com.neo4j.sync.engine.Audit or Event nodes.
                auditNode.getAudit().getAllProperties().put(propertyEntry.key(),
                        propertyEntry.previouslyCommittedValue());
                auditNode.getAudit().setPrimaryKey(getDeletedNodePrimaryKey(auditNode));

            }
            return;
        }

        auditNode = nodeChanges.get(AuditNode.NODE_PROPERTY_CHANGE);
        if (auditNode == null)
        {
            auditNode = new AuditNode();
            auditNode.getAudit().setChangeType(AuditNode.NODE_PROPERTY_CHANGE);
            auditNode.setNode(node);
            List<String> nodeLabels = new ArrayList<>();
            node.getLabels().forEach(entry -> nodeLabels.add(entry.name()));
            auditNode.getAudit().setNodeLabels(nodeLabels);
            auditNode.getAudit().setPrimaryKey(getNodePrimaryKey(node));
            // REQUIREMENT: display all the node properties to handle updates
            auditNode.getAudit().setAllProperties(node.getAllProperties());
            nodeChanges.put(AuditNode.NODE_PROPERTY_CHANGE, auditNode);

        }

        List<PropertyChange> changes = auditNode.getAudit().getProperties();
        if (changes == null)
        {
            changes = new ArrayList<>();
            auditNode.getAudit().setProperties(changes);
        }

        PropertyChange change = new PropertyChange();

        change.setPropertyName(propertyEntry.key());
        try
        {
            change.setNewValue(propertyEntry.value());
        }
        catch (Exception e)
        {
            // Ignore this exception as when the property is removed this may throw an exception.
        }
        try
        {
            change.setOldValue(propertyEntry.previouslyCommittedValue());
        }
        catch (Exception e)
        {
            // Ignore this exception as when the property is added for the first time this method
            // may throw an exception.
        }

        // Only logging the property change, if the old value and new value are not same
        if (change.getOldValue() == null
                || change.getNewValue() == null
                || !change.getOldValue().equals(change.getNewValue()))
        {
            changes.add(change);
        }
    }

    // This method is used to create audit records for relationship property change
    private void processRelationPropertyChange(PropertyEntry<Relationship> propertyEntry,
                                               Map<Node, Map<String, AuditNode>> auditValues) {
        Relationship relationship = propertyEntry.entity();
        Node node = relationship.getStartNode();
        Node endNode = relationship.getEndNode();
        Map<String, AuditNode> nodeChanges = auditValues.computeIfAbsent(node,
                k -> new HashMap<>());

        AuditNode auditNode;

        auditNode = nodeChanges.get(getRelationChangeType(AuditNode.ADD_RELATION, relationship));
        if (auditNode != null)
        {
            // There is a new node created for this. This means all the properties
            // that were added to the node using set will be called as node property changes.
            // The all properties would be set in handling the Add relation logic.
            return;
        }

        auditNode = nodeChanges.get(getRelationChangeType(AuditNode.DELETE_RELATION, relationship));
        if (auditNode != null)
        {
            // This relation is deleted. This means all the properties
            // that were part of node will come as deleted properties.
            // We need to add them to the list here as relation.getAllProperties() cannot be done.

            auditNode.getAudit().getAllProperties().put(propertyEntry.key(),
                    propertyEntry.previouslyCommittedValue());
            return;
        }

        auditNode = nodeChanges
                .get(getRelationChangeType(AuditNode.RELATION_PROPERTY_CHANGE, relationship));
        if (auditNode == null)
        {
            auditNode = new AuditNode();
            auditNode.getAudit().setChangeType(AuditNode.RELATION_PROPERTY_CHANGE);
            auditNode.setNode(node);
            auditNode.getAudit().setAllProperties(relationship.getAllProperties());

            AuditNode deletedNode = checkForDeletedNode(node, auditValues);

            if (deletedNode != null)
            {
                auditNode.getAudit().setNodeLabels(deletedNode.getAudit().getNodeLabels());
                auditNode.getAudit().setPrimaryKey(getDeletedNodePrimaryKey(deletedNode));
            }
            else
            {
                final List<String> nodeLabels = new ArrayList<>();
                node.getLabels().forEach(entry -> nodeLabels.add(entry.name()));
                auditNode.getAudit().setNodeLabels(nodeLabels);
                auditNode.getAudit().setPrimaryKey(getNodePrimaryKey(node));
            }

            auditNode.getAudit().setRelationshipLabel(relationship.getType().name());

            deletedNode = checkForDeletedNode(endNode, auditValues);
            if (deletedNode != null)
            {
                auditNode.getAudit().setTargetNodeLabels(deletedNode.getAudit().getNodeLabels());
                auditNode.getAudit().setTargetPrimaryKey(getDeletedNodePrimaryKey(deletedNode));
            }
            else
            {
                final List<String> endNodeLabels = new ArrayList<>();
                endNode.getLabels().forEach(entry -> endNodeLabels.add(entry.name()));
                auditNode.getAudit().setTargetNodeLabels(endNodeLabels);
                auditNode.getAudit().setTargetPrimaryKey(getNodePrimaryKey(endNode));
            }

            nodeChanges.put(getRelationChangeType(AuditNode.RELATION_PROPERTY_CHANGE, relationship),
                    auditNode);
        }

        List<PropertyChange> changes = auditNode.getAudit().getProperties();
        if (changes == null)
        {
            changes = new ArrayList<>();
            auditNode.getAudit().setProperties(changes);
        }

        PropertyChange change = new PropertyChange();

        change.setPropertyName(propertyEntry.key());
        try
        {
            change.setNewValue(propertyEntry.value());
        }
        catch (Exception e)
        {
            // Ignore this exception as when the property is removed this may throw an exception.
        }

        try
        {
            change.setOldValue(propertyEntry.previouslyCommittedValue());
        }
        catch (Exception e)
        {
            // Ignore this exception as when the property is added for the first time this method
            // may throw an exception.
        }

        // Only logging the relationship property change, if the old and new value are different
        if (change.getOldValue() == null
                || change.getNewValue() == null
                || !change.getOldValue().equals(change.getNewValue()))
        {
            changes.add(change);
        }
    }

    private Map<String, Object> getNodePrimaryKey(Node n)
    {
        Map<String, Object> keyValues = new HashMap<>();

        if (n.getProperty(UUID) != null) {
            keyValues.put(UUID, n.getProperty(UUID));
        }

        if (keyValues.size() == 0)
        {
            keyValues = null;
        }

        return keyValues;
    }

    private Map<String, Object> getDeletedNodePrimaryKey(AuditNode auditNode)
    {
        Map<String, Object> keyValues = new HashMap<>();

        if (auditNode.getAudit().getNodeLabels() == null)
        {
            return null;
        }

        Map<String, Object> properties = auditNode.getAudit().getAllProperties();

        if (properties != null && properties.get(UUID) != null) {
                keyValues.put(UUID, properties.get(UUID));
        }

        if (keyValues.size() == 0) {
            keyValues = null;
        }
        return keyValues;
    }

    private String getRelationChangeType(String changeType, Relationship r) {
        return String.format("%s_%d", changeType, r.getId());
    }

}
