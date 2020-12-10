package com.neo4j.sync.procedures;

import com.neo4j.sync.engine.ReplicationEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

public class StartReplicationProcedure {
    private ReplicationEngine replicationEngine;

    public StartReplicationProcedure(ReplicationEngine replicationEngine) {
        this.replicationEngine = replicationEngine;
    }

    @Procedure(value = "startReplication", mode = Mode.WRITE)
    @Description("starts the bilateral replication engine on this server.")
    public void startReplication() {
        replicationEngine.start();
    }
}
