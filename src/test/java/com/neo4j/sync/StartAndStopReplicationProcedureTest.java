package com.neo4j.sync;

import com.neo4j.sync.engine.ReplicationEngine;
import com.neo4j.sync.procedures.StartReplicationProcedure;
import com.neo4j.sync.procedures.StopReplicationProcedure;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import static org.mockito.Mockito.*;

public class StartAndStopReplicationProcedureTest {
    @Test
    public void shouldStartAndStopReplication() throws Exception {
        // Given
        ReplicationEngine replicationEngine = mock(ReplicationEngine.class);
        StartReplicationProcedure startProc = new StartReplicationProcedure(replicationEngine);
        StopReplicationProcedure stopProc = new StopReplicationProcedure(replicationEngine);

        // When
        startProc.startReplication();

        // Then
        verify(replicationEngine, times(1)).start();

        // When
        stopProc.stopReplication();

        // Then
        verify(replicationEngine, times(1)).stop();
    }
}