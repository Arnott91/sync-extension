package com.neo4j.sync.engine;

import org.junit.Test;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicationEngineTest {

    @Test
    public void shouldStartAndStopReplication() {
        // Given
        ReplicationEngine engine = ReplicationEngine.initialize("neo4j://remoteUri", "username", "password");

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
        ReplicationEngine engine = ReplicationEngine.initialize("neo4j://remoteUri", "username", "password");

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
        ReplicationEngine engine = ReplicationEngine.initialize("neo4j://remoteUri", "username", "password");

        // When
        engine.start();
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());
    }

    @Test
    public void stopShouldBeIdempotent() {
        // Given
        ReplicationEngine engine = ReplicationEngine.initialize("neo4j://remoteUri", "username", "password");

        // When
        engine.stop();
        engine.stop();

        // Then
        assertEquals(STOPPED, engine.status());
    }
}