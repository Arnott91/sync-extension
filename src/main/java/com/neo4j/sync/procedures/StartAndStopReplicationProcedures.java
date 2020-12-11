package com.neo4j.sync.procedures;

import com.neo4j.sync.engine.ReplicationEngine;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class StartAndStopReplicationProcedures
{

    private static ReplicationEngine replicationEngine;

    @Procedure( value = "startReplication", mode = Mode.WRITE )
    @Description( "starts the bilateral replication engine on this server." )
    public void startReplication(
            @Name( value = "remoteDatabaseURI" ) String remoteDatabaseURI,
            @Name( value = "username" ) String username,
            @Name( value = "password" ) String password
    )
    {
        this.replicationEngine = new ReplicationEngine( GraphDatabase.driver( remoteDatabaseURI, AuthTokens.basic( username, password ) ) );
        this.replicationEngine.start();
    }

    @Procedure( value = "stopReplication", mode = Mode.WRITE )
    @Description( "stops the bilateral replication engine on this server." )
    public void stopReplication()
    {
        this.replicationEngine.stop();
    }
}
