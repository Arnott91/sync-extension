package com.neo4j.sync.engine;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigurationTest {

    private final String CONFIG_FILE_NAME = "replication.conf";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getConfiguration() throws Exception {

        String configJSON = "{\"configuration\":{\"batchSize\":\"200\",\"outBoundTxLogFileDirectory\":\"c:/OUTBOUND_TX\"}}";

        Writer out = new FileWriter(new File(CONFIG_FILE_NAME).getAbsoluteFile());
        out.write(configJSON);
        out.close();
        assertEquals(Configuration.getBatchSize(true), 200);
    }

    @Test
    public void getConfiguration2() throws Exception {

        String configJSON = "{\"configuration\":{\"outBoundTxLogFileDirectory\":\"c:/OUTBOUND_TX\"}}";

        Writer out = new FileWriter(new File(CONFIG_FILE_NAME).getAbsoluteFile());
        out.write(configJSON);
        out.close();
        assertEquals(Configuration.getBatchSize(true), 100);
    }
}