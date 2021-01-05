package com.neo4j.sync.engine;

import org.junit.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import java.util.Set;

import static com.neo4j.sync.engine.ReplicationEngine.Status.RUNNING;
import static com.neo4j.sync.engine.ReplicationEngine.Status.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class ReplicationEngineTest {

    public static final String NEO_4_J_REMOTE_URI = "neo4j://x.example.com:7687";
    @Inject
    public GraphDatabaseAPI graphDatabaseAPI;
    String[] hostNames = {"a","b","c"};

    @Test
    public void shouldStartAndStopReplication() throws Exception {
        // Given

        ReplicationEngine engine = ReplicationEngine.initialize(NEO_4_J_REMOTE_URI, "username", "password", Set.of(hostNames));
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
    public void shouldBeAbleToRestart() throws Exception {

        // Given
        ReplicationEngine engine = ReplicationEngine.initialize(NEO_4_J_REMOTE_URI, "username", "password", Set.of(hostNames));

        // When
        engine.start();
        engine.stop();
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());

    }

    @Test
    public void startShouldBeIdempotent() throws Exception {
        // Given
        ReplicationEngine engine = ReplicationEngine.initialize(NEO_4_J_REMOTE_URI, "username", "password", Set.of(hostNames));

        // When
        engine.start();
        engine.start();

        // Then
        assertEquals(RUNNING, engine.status());
    }

    @Test
    public void stopShouldBeIdempotent() throws Exception {
        // Given
        ReplicationEngine engine = ReplicationEngine.initialize(NEO_4_J_REMOTE_URI, "username", "password", Set.of(hostNames));

        // When
        engine.stop();
        engine.stop();

        // Then
        assertEquals(STOPPED, engine.status());
    }




    public static void pause(double seconds)
    {
        try {
            Thread.sleep((long) (seconds * 1000));
        } catch (InterruptedException e) {}
    }
}