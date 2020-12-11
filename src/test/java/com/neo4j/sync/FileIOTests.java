package com.neo4j.sync;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import org.neo4j.test.extension.Inject;

@TestInstance( TestInstance.Lifecycle.PER_METHOD )

public class FileIOTests {
    @Inject
    private final String SOME_CONSTANT= "Test";

    @BeforeEach
    void setup() throws Exception
    {
        // do some setup stuff
    }

    @Test
    void shouldDoSomeUsefulThings() throws Exception
    {
        // Do work here

    }
}
