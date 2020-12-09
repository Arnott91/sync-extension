package com.neo4j.sync.engine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/***
 * com.neo4j.sync.engine.TransactionFileLogger provides static methods used to log transactions
 * and rollback information.
 * @author cupkes
 */

public class TransactionFileLogger {

    // TO_DO:  Replace hard-coded file and path info with dynamic configuration data.

    private static final String TX_LOG_FILE_DIR = "c:/OUTBOUND_TX";
    private static final String TX_RB_LOG_FILE_DIR = "c:/ROLLBACK_OUTBOUND_TX";
    private static final String TX_LOG_FILE_NAME = "outbound_tx.log";
    private static final String TX_RB_LOG_FILE_NAME = "rb_outbound_tx.log";



    private static void initializeTxLogFile(){

        File logFile = new File(TX_LOG_FILE_DIR);
        if (!logFile.exists()) {
            logFile.mkdir();
        }

    };

    private static void initializeTxRollbackLogFile(){

        File logFile = new File(TX_RB_LOG_FILE_DIR);
        if (!logFile.exists()) {
            logFile.mkdir();
        }

    };

    public static void AppendTransactionLog(String transactionData, String transactionUUID, Long transactionId, long transactionTimestamp) throws IOException {

        initializeTxLogFile();


        String logFileFullPath = TX_LOG_FILE_DIR + "/" + TX_LOG_FILE_NAME;
        String transactionRecord = transactionId + ',' + transactionUUID + ',' + transactionTimestamp + "," + transactionData + "\n";
        File logFile = new File(logFileFullPath);
        FileWriter logWriter = new FileWriter(logFile, true);


        try {

            logWriter.write(transactionRecord);


        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally
        {
            logWriter.close();
        }

    }

    public static void AppendRollbackTransactionLog(String transactionUUID, long transactionTimestamp) throws IOException {

        initializeTxRollbackLogFile();


        String rbLogFileFullPath = TX_RB_LOG_FILE_DIR + "/" + TX_RB_LOG_FILE_NAME;
        String rBTransactionRecord = transactionUUID + ',' + transactionTimestamp + "/n";
        File logFile = new File(rbLogFileFullPath);
        FileWriter logWriter = new FileWriter(logFile, true);


        try {

            logWriter.write(rBTransactionRecord);


        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally
        {
            logWriter.close();
        }

    }



}