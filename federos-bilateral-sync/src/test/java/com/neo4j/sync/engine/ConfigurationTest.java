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


}