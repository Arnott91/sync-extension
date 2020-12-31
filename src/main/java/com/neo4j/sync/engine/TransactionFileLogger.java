package com.neo4j.sync.engine;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
    // TO_DO:  Replace hard-coded file and path info with dynamic configuration data.

    private static String TX_LOG_FILE_DIR = "c:/OUTBOUND_TX";
    private static String TX_RB_LOG_FILE_DIR = "c:/ROLLBACK_OUTBOUND_TX";
    private static String IB_TX_LOG_FILE_DIR = "c:/INBOUND_TX;";
    private static String POLL_LOG_FILE_DIR = "c:/POLLING";
    private static String TX_LOG_FILE_NAME_IN = "inbound_tx.log";
    private static String TX_LOG_FILE_NAME = "outbound_tx.log";
    private static String TX_RB_LOG_FILE_NAME = "rb_outbound_tx.log";
    private static String POLLING_LOG = "tx_poll.log";
    private static boolean areSettingsInitialized = false;

    private static void initializeTxLogFile() {

        File logFile = new File(TX_LOG_FILE_DIR);
        if (!logFile.exists()) {
            logFile.mkdir();
        }
    }

    private static void initializeTxRollbackLogFile() {

        File logFile = new File(TX_RB_LOG_FILE_DIR);
        if (!logFile.exists()) {
            logFile.mkdir();
        }
    }

    private static void initializeTxLogFile(Logtype logtype) throws IOException {

        String fileFullPath = null;
        File logFile = null;

        switch (logtype) {
            case INBOUND_TX: fileFullPath = IB_TX_LOG_FILE_DIR;
            break;
            case OUTBOUND_TX: fileFullPath = TX_LOG_FILE_DIR;
            break;
            case TX_POLLING: fileFullPath = POLL_LOG_FILE_DIR;
        }

        if (fileFullPath != null) logFile = new File(fileFullPath);
        if (!logFile.exists()) {
            logFile.mkdir();
            boolean newFile = logFile.createNewFile();


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

    public static void AppendTransactionLog(String transactionData, String transactionUUID, long transactionId, long transactionTimestamp, Log log) {

        initializeTxLogFile();

        String logFileFullPath = TX_LOG_FILE_DIR + "/" + TX_LOG_FILE_NAME;
        String transactionRecord = transactionId + ',' + transactionUUID + ',' + transactionTimestamp + "," + transactionData + "\n";
        File logFile = new File(logFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(transactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void AppendTransactionLog(String transactionData, String transactionUUID, long transactionId, long transactionTimestamp) {

        initializeTxLogFile();

        String logFileFullPath = TX_LOG_FILE_DIR + "/" + TX_LOG_FILE_NAME;
        String transactionRecord = transactionId + ',' + transactionUUID + ',' + transactionTimestamp + "," + transactionData + "\n";
        File logFile = new File(logFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(transactionRecord);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void AppendRollbackTransactionLog(String transactionUUID, long transactionTimestamp, Log log) {

        initializeTxRollbackLogFile();

        String rbLogFileFullPath = TX_RB_LOG_FILE_DIR + "/" + TX_RB_LOG_FILE_NAME;
        String rBTransactionRecord = transactionUUID + ',' + transactionTimestamp + "\n";
        File logFile = new File(rbLogFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(rBTransactionRecord);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void AppendRollbackTransactionLog(String transactionUUID, long transactionTimestamp) {

        initializeTxRollbackLogFile();

        String rbLogFileFullPath = TX_RB_LOG_FILE_DIR + "/" + TX_RB_LOG_FILE_NAME;
        String rBTransactionRecord = transactionUUID + ',' + transactionTimestamp + "\n";
        File logFile = new File(rbLogFileFullPath);

        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(rBTransactionRecord);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void AppendPollingLog(String message) throws IOException {

        initializeTxLogFile(Logtype.TX_POLLING);
        String pollMessage = message + "\n";

        String pollLogFileFullPath =  POLL_LOG_FILE_DIR + "/" + POLLING_LOG;
        File logFile = new File(pollLogFileFullPath);
        try (FileWriter logWriter = new FileWriter(logFile, true)) {
            logWriter.write(pollMessage);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }



    }

    public static boolean isAreSettingsInitialized() {
        return areSettingsInitialized;
    }
}