import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
// import org.codehaus.jackson.map.ObjectMapper; // need to add the jackson library to the maven dependencies
import org.neo4j.graphdb.*;
import org.apache.commons.collections.IteratorUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
// import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;


public class TransactionRecorder {

    public static final Label EVENT_NODE_LABEL = Label
            .label("Event");
    public static final Label REP_NODE_LABEL = Label
            .label("ReplicationDB");
    private static final String DONT_REPLICATE = "dont_replicate";
    public static final Label NO_AUDIT_NODE_LABEL = Label
            .label(DONT_REPLICATE);
    private static final String UUID = "uuid";
    private static Log log;
    private static boolean enabled = true;
    private static boolean replicationEnabled = true;
    private static boolean eventEnabled = true;
    private final TransactionData transactionData;

    public TransactionRecorder(TransactionData txData) throws Exception {
        this.transactionData = txData;


        /*ObjectMapper objectMapper = new ObjectMapper();
        try {
            if (!enabled) {
                log.debug("Trigger is disabled. Not processing TransactionData");
                return null;
            }

            StopWatch timer = StopWatch.createStarted();

            long timestamp = OffsetDateTime.now(ZoneOffset.UTC).toEpochSecond();
            Map<Node, Node> nodeChain = new HashMap<>();
            // NEED to understand the audit node class
            // In Ravi's code he uses an actual Neo4j node to store audit properties
            //
            Map<Object, List<AuditNode>> deletedRelationsMap = new HashMap<>();
            List<AuditNode> unIdentifiedDeleteRelations = new ArrayList<>();
            List<Node> deletedHouseholdNodes = new ArrayList<>();
            Set<Object> householdIds = new HashSet<>();

            // Keeps track of deleted Nodes changes by householdUuid
            Map<Object, Map<String, Map<String, Object>>> deletedNodeAndLabelMap = new HashMap<>();

            // See if transaction contains a DONT_AUDIT node. Stop processing if so


            for (Node node : transactionData.createdNodes()) {
                Iterable<Label> labels = node.getLabels();
                for (Label l : labels) {
                    if (l.name().equalsIgnoreCase(DONT_REPLICATE)) {
                        // Delete the DONT_AUDIT node so that it's not peristed to the database
                        // TO_DO  let's change this to check and see if a db node was merged
                        // as part of the transaction. i.e. MERGE (repdb:ReplicationDB {number:1})
                        node.delete();

                        log.debug("Transaction contained a %s node so not replicating", DONT_REPLICATE);
                        return null;
                    }
                }
            }

            for (Node createdNode : transactionData.createdNodes()) {
                String householdUuid = getAssociatedHouseHoldId(createdNode);
                if (householdUuid != null) {
                    affectedNodesToHouseholdUuidMap.put(createdNode, householdUuid);
                    householdIds.add(householdUuid);
                }

                if (auditEnabled) {
                    processAddRemoveNode(createdNode, auditValues, AuditNode.ADD_NODE,
                            transactionData, affectedNodesToHouseholdUuidMap, deletedHouseholdNodes);
                }
            }

            for (Node deletedNode : transactionData.deletedNodes()) {
                processAddRemoveNode(deletedNode, auditValues, AuditNode.DELETE_NODE,
                        transactionData, affectedNodesToHouseholdUuidMap, deletedHouseholdNodes);
            }

            for (PropertyEntry<Node> propertyEntry : transactionData.assignedNodeProperties()) {
                String householdUuid = getAssociatedHouseHoldId(propertyEntry.entity());
                if (householdUuid != null) {
                    affectedNodesToHouseholdUuidMap.put(propertyEntry.entity(), householdUuid);
                    householdIds.add(householdUuid);
                }
                if (auditEnabled) {
                    processNodePropertyChange(propertyEntry, auditValues,
                            affectedNodesToHouseholdUuidMap, deletedHouseholdNodes);
                }
            }

            for (PropertyEntry<Node> propertyEntry : transactionData.removedNodeProperties()) {
                Node n = propertyEntry.entity();
                AuditNode aNode = checkForDeletedNode(n, auditValues);
                if (aNode == null) {
                    String householdUuid = getAssociatedHouseHoldId(n);
                    if (householdUuid != null) {
                        affectedNodesToHouseholdUuidMap.put(n, householdUuid);
                        householdIds.add(householdUuid);
                    }
                }

                // We do have to process the Deleted node property changes
                // to be able to handle the household node delete scenario.
                // Otherwise it would be missed in the events. If the household
                // is never going to be deleted then we can add audit flag check.
                processNodePropertyChange(propertyEntry, auditValues,
                        affectedNodesToHouseholdUuidMap, deletedHouseholdNodes);
            }

            for (Relationship createdRelationship : transactionData.createdRelationships()) {
                nodeChain.put(createdRelationship.getEndNode(), createdRelationship.getStartNode());
                String householdUuid = getAssociatedHouseHoldId(createdRelationship.getStartNode());
                if (householdUuid != null) {
                    affectedNodesToHouseholdUuidMap.put(createdRelationship.getStartNode(),
                            householdUuid);
                    affectedNodesToHouseholdUuidMap.put(createdRelationship.getEndNode(),
                            householdUuid);
                    householdIds.add(householdUuid);
                }

                if (auditEnabled) {
                    processAddRemoveRelationship(createdRelationship, auditValues,
                            AuditNode.ADD_RELATION, affectedNodesToHouseholdUuidMap);
                }
            }

            for (Relationship deletedRelationship : transactionData.deletedRelationships()) {
                Node startNode = deletedRelationship.getStartNode();
                Node endNode = deletedRelationship.getEndNode();
                nodeChain.put(endNode, startNode);

                AuditNode aNode = checkForDeletedNode(startNode, auditValues);
                if (aNode == null) {
                    String householdUuid = getAssociatedHouseHoldId(startNode);
                    if (householdUuid != null) {
                        affectedNodesToHouseholdUuidMap.put(startNode, householdUuid);
                        affectedNodesToHouseholdUuidMap.put(endNode, householdUuid);
                        householdIds.add(householdUuid);
                    }
                }

                // We want to capture the Deleted relations for Event processing.
                // This information is used to keep the other data sources relationships in sync
                // with Neo4J.
                AuditNode auditNode = processAddRemoveRelationship(deletedRelationship, auditValues,
                        AuditNode.DELETE_RELATION, affectedNodesToHouseholdUuidMap);
                if (auditNode.getAudit().getHouseholdUuid() != null) {
                    List<AuditNode> data = deletedRelationsMap.computeIfAbsent(
                            auditNode.getAudit().getHouseholdUuid(), k -> new ArrayList<>());
                    data.add(auditNode);
                } else {
                    unIdentifiedDeleteRelations.add(auditNode);
                }
            }

            for (PropertyEntry<Relationship> propertyEntry : transactionData
                    .assignedRelationshipProperties()) {
                nodeChain.put(propertyEntry.entity().getEndNode(),
                        propertyEntry.entity().getStartNode());
                String householdUuid = getAssociatedHouseHoldId(
                        propertyEntry.entity().getStartNode());
                if (householdUuid != null) {
                    affectedNodesToHouseholdUuidMap.put(propertyEntry.entity().getStartNode(),
                            householdUuid);
                    affectedNodesToHouseholdUuidMap.put(propertyEntry.entity().getEndNode(),
                            householdUuid);
                    householdIds.add(householdUuid);
                }

                if (auditEnabled) {
                    processRelationPropertyChange(propertyEntry, auditValues,
                            affectedNodesToHouseholdUuidMap);
                }
            }

            for (PropertyEntry<Relationship> propertyEntry : transactionData
                    .removedRelationshipProperties()) {
                nodeChain.put(propertyEntry.entity().getEndNode(),
                        propertyEntry.entity().getStartNode());
                Node n = propertyEntry.entity().getStartNode();
                if (checkForDeletedNode(n, auditValues) == null) {
                    String householdUuid = getAssociatedHouseHoldId(n);
                    if (householdUuid != null) {
                        affectedNodesToHouseholdUuidMap.put(propertyEntry.entity().getStartNode(),
                                householdUuid);
                        affectedNodesToHouseholdUuidMap.put(propertyEntry.entity().getEndNode(),
                                householdUuid);
                        householdIds.add(householdUuid);
                    }
                }

                if (auditEnabled) {
                    processRelationPropertyChange(propertyEntry, auditValues,
                            affectedNodesToHouseholdUuidMap);
                }
            }

            // Get the deleted distinct household Id's from deleted nodes.
            for (Map<String, AuditNode> auditProps : auditValues.values()) {
                AuditNode auditNode = auditProps.get(AuditNode.DELETE_NODE);
                if (auditNode != null) {
                    String householdUuid = auditNode.getAudit().getHouseholdUuid();
                    if (householdUuid == null) {
                        householdUuid = getHouseholdUuidFromChain(auditNode.getNode(), nodeChain,
                                affectedNodesToHouseholdUuidMap);
                    }
                    if (householdUuid != null) {
                        auditNode.getAudit().setHouseholdUuid(householdUuid);
                    }

                    if (auditNode.getAudit().getNodeLabels() != null
                            && auditNode.getAudit().getNodeLabels().size() > 0
                            && auditNode.getAudit().getNodeLabels().get(0).equals(HOUSEHOLD_LABEL)) {
                        Object o = auditNode.getAudit().getAllProperties().get(UUID);
                        if (o != null) {
                            if (auditNode.getNode() != null) {
                                affectedNodesToHouseholdUuidMap.put(auditNode.getNode(),
                                        o.toString());
                            }
                            householdIds.add(o.toString());
                        }
                    }
                }
            }

            // Get the deleted distinct household Id's from deleted nodes.
            for (Map<String, AuditNode> auditProps : auditValues.values()) {
                AuditNode auditNode = auditProps.get(AuditNode.DELETE_NODE);
                if (auditNode != null) {
                    String householdUuid = auditNode.getAudit().getHouseholdUuid();
                    if (householdUuid == null) {
                        householdUuid = getHouseholdUuidFromChain(auditNode.getNode(), nodeChain,
                                affectedNodesToHouseholdUuidMap);
                    }
                    if (householdUuid != null) {
                        auditNode.getAudit().setHouseholdUuid(householdUuid);
                    }

                    // For tracking deleted Nodes for events
                    // holds the node label and deletedNodePrimaryKey map
                    Map<String, Map<String, Object>> nodeLabelAndDeletedNodePrimaryKeyMap = new HashMap<>();
                    nodeLabelAndDeletedNodePrimaryKeyMap.put(
                            auditNode.getAudit().getNodeLabels().get(0),
                            getDeletedNodePrimaryKey(auditNode));

                    // only capture the deleted Nodes that have household
                    // we are checking if the householdId is already stored in a map so that
                    // we can store the collections of values for the key
                    if (!deletedNodeAndLabelMap.isEmpty()
                            && deletedNodeAndLabelMap.containsKey(householdUuid)) {
                        Map<String, Map<String, Object>> tempDeletedNodesMap = new HashMap<>();
                        tempDeletedNodesMap.putAll(deletedNodeAndLabelMap.get(householdUuid));

                        String deletedNodeKey = nodeLabelAndDeletedNodePrimaryKeyMap.entrySet()
                                .iterator().next().getKey();
                        Map<String, Object> deletedNodeValue = nodeLabelAndDeletedNodePrimaryKeyMap
                                .entrySet().iterator().next().getValue();

                        // in order to capture the multi values for the key
                        // we are storing the in temporary map and processing the values
                        if (tempDeletedNodesMap.containsKey(deletedNodeKey)) {
                            for (Map.Entry<String, Map<String, Object>> entry : tempDeletedNodesMap
                                    .entrySet()) {
                                if (entry.getKey().equals(deletedNodeKey)) {
                                    Map<String, Object> primaryKeyMap = entry.getValue();

                                    if (primaryKeyMap.entrySet().iterator().next().getKey().equals(
                                            deletedNodeValue.entrySet().iterator().next().getKey())) {
                                        List<Object> list = new ArrayList<>();
                                        list.add(primaryKeyMap.values().iterator().next());
                                        list.add(deletedNodeValue.values().iterator().next());

                                        Map<String, Object> primaryKeyMultiValueMap = new HashMap<>();
                                        primaryKeyMultiValueMap.put(
                                                primaryKeyMap.entrySet().iterator().next().getKey(),
                                                StringUtils.join(list, ","));

                                        tempDeletedNodesMap.put(deletedNodeKey,
                                                primaryKeyMultiValueMap);
                                    }
                                }
                            }
                        } else {
                            tempDeletedNodesMap.putAll(nodeLabelAndDeletedNodePrimaryKeyMap);
                        }

                        deletedNodeAndLabelMap.put(householdUuid, tempDeletedNodesMap);
                    } else {
                        if (householdUuid == null) {
                            householdUuid = getHouseholdUuidFromChain(auditNode.getNode(),
                                    nodeChain, affectedNodesToHouseholdUuidMap);
                        }
                        if (householdUuid != null) {
                            deletedNodeAndLabelMap.put(householdUuid,
                                    nodeLabelAndDeletedNodePrimaryKeyMap);
                        }
                    }
                }
            }

            for (AuditNode auditNode : unIdentifiedDeleteRelations) {
                // By this time if for any of these audit nodes the Household ID
                // should have been calculated
                if (auditNode.getAudit().getHouseholdUuid() != null) {
                    List<AuditNode> data = deletedRelationsMap.computeIfAbsent(
                            auditNode.getAudit().getHouseholdUuid(), k -> new ArrayList<>());
                    data.add(auditNode);
                }
            }

            if (eventEnabled) {
                // Generates event for each household
                for (Object householdId : householdIds) {
                    Node eventNode = db.createNode(EVENT_NODE_LABEL);
                    eventNode.setProperty(UUID, householdId);
                    eventNode.setProperty("timestamp", timestamp);
                    eventNode.setProperty("status", "NEW");

                    if (!deletedNodeAndLabelMap.isEmpty()
                            && deletedNodeAndLabelMap.containsKey(householdId)) {
                        eventNode.setProperty("deletedNode", objectMapper.writeValueAsString(
                                deletedNodeAndLabelMap.get(householdId).entrySet()));
                    }

                    // For capturing deleted relationship
                    List<AuditNode> deletedRelations = deletedRelationsMap.get(householdId);
                    if (deletedRelations != null) {
                        // Write the Deleted Relations Data
                        List<Map<String, Object>> deleteRelationsData = new ArrayList<>();
                        for (AuditNode auditNode : deletedRelations) {
                            Map<String, Object> relData = new HashMap<>();
                            relData.put("startNodeKey", auditNode.getAudit().getPrimaryKey());
                            relData.put("startNodeLabel", auditNode.getAudit().getNodeLabels());
                            relData.put("endNodeKey", auditNode.getAudit().getTargetPrimaryKey());
                            relData.put("endNodeLabel", auditNode.getAudit().getTargetNodeLabels());
                            relData.put("relationship",
                                    auditNode.getAudit().getRelationshipLabel());
                            if (auditNode.getAudit().getAllProperties() != null) {
                                relData.put("properties", auditNode.getAudit().getAllProperties());
                            } else if (auditNode.getAudit().getProperties() != null) {
                                relData.put("properties", auditNode.getAudit().getProperties());
                            }
                            deleteRelationsData.add(relData);
                        }
                        eventNode.setProperty("deletedRelations",
                                objectMapper.writeValueAsString(deleteRelationsData));
                    }
                }
            }

            // Update the indexed search properties of all of the affected Households
            Set<String> processedHouseholdUuids = new HashSet<>();
            String curHouseholdUuid;
            StopWatch indexUpdateTimer = StopWatch.createStarted();

            for (Node affectedNode : affectedNodesToHouseholdUuidMap.keySet()) {
                curHouseholdUuid = affectedNodesToHouseholdUuidMap.get(affectedNode);
                if (!processedHouseholdUuids.contains(curHouseholdUuid)) {
                    processedHouseholdUuids.add(curHouseholdUuid);

                    populateSearchProperties(getAssociatedHouseholdNode(affectedNode));
                }
            }

            log.debug("Updated indexed properties for %d housholds in %d ms",
                    processedHouseholdUuids.size(), indexUpdateTimer.getTime());

            if (auditEnabled) {
                // Generates audit
                List<AuditNode> nodesToProcess = new ArrayList<>();
                String transactionUUID = java.util.UUID.randomUUID().toString();
                for (Map<String, AuditNode> auditProps : auditValues.values()) {
                    for (AuditNode auditNode : auditProps.values()) {
                        if (auditNode.getAudit().getPrimaryKey() == null) {
                            // This is most likely a deleted node or Add node. Let's check the
                            // primary
                            // key
                            // can be obtained for this node. If not we need to ignore the node.
                            if (getDeletedNodePrimaryKey(auditNode) == null) {
                                continue;
                            }
                        }

                        String householdUuid = auditNode.getAudit().getHouseholdUuid();
                        if (householdUuid == null) {
                            householdUuid = getHouseholdUuidFromChain(auditNode.getNode(),
                                    nodeChain, affectedNodesToHouseholdUuidMap);
                        }
                        if (householdUuid != null) {
                            auditNode.getAudit().setHouseholdUuid(householdUuid);
                        } else {
                            // We don't want to generate audit that does not have householdId
                            continue;
                        }

                        auditNode.setNode(null);
                        nodesToProcess.add(auditNode);
                    }
                }

                if (!nodesToProcess.isEmpty()) {
                    Node aNode = db.createNode(AUDIT_NODE_LABEL);
                    aNode.setProperty("timestamp", timestamp);
                    aNode.setProperty("transactionId", transactionUUID);
                    aNode.setProperty("status", "NEW");

                    List<Audit> audits = new ArrayList<>();
                    for (AuditNode auditNode : nodesToProcess) {
                        audits.add(auditNode.getAudit());
                    }

                    aNode.setProperty("data", objectMapper.writeValueAsString(audits));
                }
            }

            log.debug("Processed %d Households in %d ms", householdIds.size(), timer.getTime());
        } catch (Exception e) {
            log.error(
                    "Caught a %s in FederosTransactionHandler.beforeCommit(). Msg: %s\nStack trace:\n%s",
                    e.getClass().getName(), e.getMessage(), ExceptionUtils.getStackTrace(e));

            // explicitly re-throw the exception to indicate that the transaction needs to be rolled
            // back
            throw e;
        }
        return null;*/
    }
}
