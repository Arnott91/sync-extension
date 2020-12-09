package com.neo4j.sync;

import com.neo4j.sync.procedures.StartReplicationProcedure;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;

import static com.neo4j.sync.engine.ReplicationEngine.replicationEngine;
import static org.mockito.Mockito.*;

class StartReplicationProcedureTest {
    @Test
    public void shouldStartReplication() throws Exception {
        // Given
        Driver driver = mock(Driver.class);
        StartReplicationProcedure proc = new StartReplicationProcedure(replicationEngine(driver));

        // When
        proc.startReplication();

        // Then
        verify(driver, times(1)).session();
    }
}