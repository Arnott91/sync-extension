package com.neo4j.sync.engine;

import org.neo4j.logging.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/***
 * com.neo4j.sync.engine.TransactionFileLogger provides static methods used to log transactions
 * and rollback information.
 * @author cupkes
 */

public class TransactionFileLogger {
    // TODO:  Replace hard-coded file and path info with dynamic configuration data.

    private static String TX_LOG_FILE_DIR = "/var/lib/neo4j/data/OUTBOUND_TX";
    private static String TX_RB_LOG_FILE_DIR = "/var/lib/neo4j/data/ROLLBACK_OUTBOUND_TX";
    private static String IB_TX_LOG_FILE_DIR = "/var/lib/neo4j/data/INBOUND_TX;";
    private static String POLL_LOG_FILE_DIR = "/var/lib/neo4j/data/POLLING";
    private static String TX_LOG_FILE_NAME_IN = "inbound_tx.log";
    private static String TX_LOG_FILE_NAME = "outbound_tx.log";
    private static String TX_RB_LOG_FILE_NAME = "rb_outbound_tx.log";
    private static String POLLING_LOG = "tx_poll.log";
    private static boolean areSettingsInitialized = false;

    private TransactionFileLogger() {
        // private constructor to hide implicit public one
    }

    private static void initializeTxLogFile(Log log) {

        File logFile = new File(TX_LOG_FILE_DIR);
        if (!logFile.exists()) {
            createDir(logFile, log);
        }
    }

    private static void initializeTxRollbackLogFile(Log log) {

        File logFile = new File(TX_RB_LOG_FILE_DIR);
        if (!logFile.exists()) {
            createDir(logFile, log);
        }
    }

    private static void initializeTxLogFile(LogType logtype, Log log) throws IOException {

        String fileFullPath = null;
        File logFile;

        switch (logtype) {
            case INBOUND_TX:
                fileFullPath = IB_TX_LOG_FILE_DIR;
                break;
            case OUTBOUND_TX:
                fileFullPath = TX_LOG_FILE_DIR;
                break;
            case TX_POLLING:
                fileFullPath = POLL_LOG_FILE_DIR;
                break;
            default:
                break;
        }

        if (fileFullPath != null) {
            logFile = new File(fileFullPath);

            if (!logFile.exists()) {
                createDir(logFile, log);
                createLogFile(logFile, log);
            } else {
                log.debug("TransactionFileLogger -> Using existing log file");
            }

        } else {
            throw new IOException("No file path specified for log file!");
        }
    }

    private static void createDir(File logFile, Log log) {
        if (logFile.mkdir()) {
            log.debug("TransactionFileLogger -> Log directory created: %s", logFile.getPath());
        }
    }

    private static void createLogFile(File logFile, Log log) throws IOException {
        if (logFile.createNewFile()) {
            log.debug("TransactionFileLogger -> Log file created");
        }
    }

    public static void initSettings(Map<String, Object> settings) {

        TX_RB_LOG_FILE_NAME = settings.get("TX_RB_LOG_FILE_NAME").toString();
        TX_LOG_FILE_DIR = settings.get("TX_LOG_FILE_DIR").toString();
        IB_TX_LOG_FILE_DIR = settings.get("IB_TX_LOG_FILE_DIR").toString();
        TX_RB_LOG_FILE_DIR = settings.get("TX_RB_LOG_FILE_DIR").toString();
        POLL_LOG_FILE_DIR = settings.get("POLL_LOG_FILE_DIR").toString();
        TX_LOG_FILE_NAME_IN = settings.get("TX_LOG_FILE_NAME_IN").toString();
        TX_LOG_FILE_NAME = settings.get("TX_LOG_FILE_NAME").toString();
        POLLING_LOG = settings.get("POLLING_LOG").toString();

    }

    public static void appendTransactionLog(String transactionData, String transactionUUID, long transactionId, long transactionTimestamp, Log log) {

        initializeTxLogFile(log);

        String logFileFullPath = TX_LOG_FILE_DIR + "/" + TX_LOG_FILE_NAME;
        String transactionRecord = transactionId + ',' + transactionUUID + ',' + transactionTimestamp + "," + transactionData + "\n";
        File logFile = new File(logFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(transactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void appendRollbackTransactionLog(String transactionUUID, long transactionTimestamp, Log log) {

        initializeTxRollbackLogFile(log);

        String rbLogFileFullPath = TX_RB_LOG_FILE_DIR + "/" + TX_RB_LOG_FILE_NAME;
        String rBTransactionRecord = transactionUUID + ',' + transactionTimestamp + "\n";
        File logFile = new File(rbLogFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(rBTransactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void appendPollingLog(String message, Log log) throws IOException {

        initializeTxLogFile(LogType.TX_POLLING, log);
        String pollMessage = message + "\n";

        String pollLogFileFullPath =  POLL_LOG_FILE_DIR + "/" + POLLING_LOG;
        File logFile = new File(pollLogFileFullPath);
        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(pollMessage);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }



    }

    public static boolean isAreSettingsInitialized() {
        return areSettingsInitialized;
    }
}