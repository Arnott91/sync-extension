package com.neo4j.sync.engine;

import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.graphdb.GraphDatabaseService;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class ReplicationEngineTest {

    @Test
    public void shouldStartAndStopReplication()
    {
        // Given
        ReplicationEngine engine = new ReplicationEngine(mock(Driver.class), mock(GraphDatabaseService.class));

        // When
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());

        // When
        engine.stop();

        // Then
        assertEquals(STOPPED, engine.status());
    }

    @Test
    public void shouldBeAbleToRestart() {
        // Given
        ReplicationEngine engine = new ReplicationEngine(mock(Driver.class), mock(GraphDatabaseService.class));

        // When
        engine.start();
        engine.stop();
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());
    }

    @Test
    public void startShouldBeIdempotent() {
        // Given
        ReplicationEngine engine = new ReplicationEngine(mock(Driver.class), mock(GraphDatabaseService.class));

        // When
        engine.start();
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());
    }

    @Test
    public void stopShouldBeIdempotent() {
        // Given
        ReplicationEngine engine =  new ReplicationEngine(mock(Driver.class), mock(GraphDatabaseService.class));

        // When
        engine.stop();
        engine.stop();

        // Then
        assertEquals(STOPPED, engine.status());
    }
}