package com.neo4j.sync.engine;

import java.util.List;
import java.util.Map;


/**
 * com.neo4j.sync.engine.Audit contains information about changes made to an object. It also contains metadata about who
 * made the change and when.
 *
 * @authors Chris Upkes
 */

public class Audit
{
    private String               changeType;
    private List<String>         nodeLabels;
    private Map<String, Object>  primaryKey;
    private Map<String, Object>  nodeKey;
    private String               relationshipLabel;
    private List<String>         targetNodeLabels;
    private Map<String, Object>  targetPrimaryKey;
    private List<PropertyChange> properties;
    private Map<String, Object>  allProperties;
    private String               uuid;
    private Long                 timestamp;
    private String               transactionId;
    private Map<String, Object>  targetNodeKey;

    public Map<String, Object> getNodeKey()
    {
        return nodeKey;
    }

    public void setNodeKey(Map<String, Object> nodeKey)
    {
        this.nodeKey = nodeKey;
    }

    public Map<String, Object> getPrimaryKey()
    {
        return primaryKey;
    }

    public void setPrimaryKey(Map<String, Object> primaryKey)
    {
        this.primaryKey = primaryKey;
    }

    public String getChangeType()
    {
        return changeType;
    }

    public void setChangeType(String changeType)
    {
        this.changeType = changeType;
    }

    public List<String> getNodeLabels()
    {
        return nodeLabels;
    }

    public void setNodeLabels(List<String> nodeLabels)
    {
        this.nodeLabels = nodeLabels;
    }

    public List<PropertyChange> getProperties()
    {
        return properties;
    }

    public void setProperties(List<PropertyChange> properties)
    {
        this.properties = properties;
    }

    public String getUuid()
    {
        return uuid;
    }

    public void setUuid(String uuid)
    {
        this.uuid = uuid;
    }

    public String getRelationshipLabel()
    {
        return relationshipLabel;
    }

    public void setRelationshipLabel(String relationshipLabel)
    {
        this.relationshipLabel = relationshipLabel;
    }

    public List<String> getTargetNodeLabels()
    {
        return targetNodeLabels;
    }

    public void setTargetNodeLabels(List<String> targetNodeLabels)
    {
        this.targetNodeLabels = targetNodeLabels;
    }

    public Map<String, Object> getTargetPrimaryKey()
    {
        return targetPrimaryKey;
    }

    public void setTargetPrimaryKey(Map<String, Object> targetPrimaryKey)
    {
        this.targetPrimaryKey = targetPrimaryKey;
    }

    public Map<String, Object> getAllProperties()
    {
        return allProperties;
    }

    public void setAllProperties(Map<String, Object> allProperties)
    {
        this.allProperties = allProperties;
    }

    public Long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getTransactionId()
    {
        return transactionId;
    }

    public void setTransactionId(String transactionId)
    {
        this.transactionId = transactionId;
    }

    @Override
    public String toString()
    {
        return "com.neo4j.sync.engine.Audit{"
                + "primaryKey="
                + primaryKey
                + "nodeKey="
                + nodeKey
                + ", changeType='"
                + changeType
                + '\''
                + ", nodeLabels="
                + nodeLabels
                + ", properties="
                + properties
                + ", householdId='"
                + uuid
                + '\''
                + ", relationshipLabel='"
                + relationshipLabel
                + '\''
                + ", targetNodeLabels="
                + targetNodeLabels
                + ", targetPrimaryKey="
                + targetPrimaryKey
                + ", allProperties="
                + allProperties
                + ", timestamp="
                + timestamp
                + ", transactionId='"
                + transactionId
                + '\''
                + '}';
    }

    public Map<String, Object> getTargetNodeKey()
    {
        return targetNodeKey;
    }

    public void setTargetNodeKey(Map<String, Object> targetNodeKey)
    {
        this.targetNodeKey = targetNodeKey;
    }
}
