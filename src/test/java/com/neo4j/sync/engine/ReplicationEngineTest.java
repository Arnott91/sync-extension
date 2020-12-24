package com.neo4j.sync.engine;

import org.junit.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ReplicationEngineTest {

    @Inject
    public GraphDatabaseAPI graphDatabaseAPI;

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



    @Test
    public void shouldBeLogging() throws Exception{
        // Given
        assertNotNull(graphDatabaseAPI);
        ReplicationEngine engine = ReplicationEngine.initialize("neo4j://remoteUri", "username", "password", graphDatabaseAPI);

        // When
        engine.start2();
        //pause(120);



        // Then
        //assertEquals(STOPPED, engine.status());
    }

    public static void pause(double seconds)
    {
        try {
            Thread.sleep((long) (seconds * 1000));
        } catch (InterruptedException e) {}
    }
}