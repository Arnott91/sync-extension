package com.neo4j.sync.procedures;

import com.neo4j.sync.engine.ReplicationEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;


public class StopReplicationProcedure {
    private ReplicationEngine replicationEngine;

    public StopReplicationProcedure(ReplicationEngine replicationEngine) {
        this.replicationEngine = replicationEngine;
    }

    @Procedure(value = "stopReplication", mode = Mode.WRITE)
    @Description("stops the bilateral replication engine on this server.")
    public void stopReplication() {
        replicationEngine.stop();
    }
}
