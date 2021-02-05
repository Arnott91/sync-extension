package com.neo4j.sync.engine;

import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/***
 * com.neo4j.sync.engine.TransactionFileLogger provides static methods used to log transactions
 * and rollback information.
 * @author cupkes
 */

public class TransactionFileLogger {

    private static boolean settingsInitialized = false;

    private static String txInLogFile;
    private static String txOutLogFile;
    private static String txRbLogFile;
    private static String pollingLog;

    private TransactionFileLogger() {
        // private constructor to hide implicit public one
    }

    private static void initLogFile(String fullFilePath, Log log) {
        File logFile = new File(fullFilePath);

        if (!logFile.exists()) {
            createDir(logFile, log);
        }
    }

    private static void createDir(File logFile, Log log) {
        if (logFile.getParentFile().mkdirs()) {
            log.debug("TransactionFileLogger -> Log directory created: %s", logFile.getPath());
        }
    }

    public static void initSettings(Log log) {
        if (!settingsInitialized) {
            if (Configuration.isNotInitialized()) {
                Configuration.initializeFromNeoConf(log);
                Configuration.logSettings();
            }
            txOutLogFile = Configuration.getTxOutLogFile();
            txInLogFile = Configuration.getTxInLogFile();
            txRbLogFile = Configuration.getTxRbLogFile();
            pollingLog = Configuration.getPollingLog();

            settingsInitialized = true;
        }
    }

    public static void appendOutTransactionLog(String transactionData, String transactionUUID, long transactionId, long transactionTimestamp, Log log) {

        initSettings(log);
        initLogFile(txOutLogFile, log);

        String transactionRecord = transactionId + ',' + transactionUUID + ',' + transactionTimestamp + "," + transactionData + "\n";
        File logFile = new File(txOutLogFile);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(transactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void appendRollbackTransactionLog(String transactionUUID, long transactionTimestamp, Log log) {

        initSettings(log);
        initLogFile(txRbLogFile, log);

        String rBTransactionRecord = transactionUUID + ',' + transactionTimestamp + "\n";
        File logFile = new File(txRbLogFile);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(rBTransactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void appendPollingLog(String message, Log log) throws IOException {

        initSettings(log);
        initLogFile(pollingLog, log);
        String pollMessage = message + "\n";

        File logFile = new File(pollingLog);
        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(pollMessage);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static boolean areSettingsInitialized() {
        return settingsInitialized;
    }
}