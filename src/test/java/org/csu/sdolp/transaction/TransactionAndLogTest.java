package org.csu.sdolp.transaction;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransactionAndLogTest {

    private final String TEST_DB_FILE_NAME = "test_wal.db";
    private final String TEST_LOG_FILE_NAME = "test_wal.db.log";

    private DiskManager diskManager;
    private BufferPoolManager bufferPoolManager;
    private LogManager logManager;
    private LockManager lockManager;
    private TransactionManager transactionManager;
    private Catalog catalog;
    private TableInfo testTableInfo;
    private Schema testTableSchema;

    @BeforeEach
    void setUp() throws IOException {
        new File(TEST_DB_FILE_NAME).delete();
        new File(TEST_LOG_FILE_NAME).delete();

        diskManager = new DiskManager(TEST_DB_FILE_NAME);
        diskManager.open();
        bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        logManager = new LogManager(TEST_LOG_FILE_NAME);
        lockManager = new LockManager();
        transactionManager = new TransactionManager(lockManager, logManager);
        catalog = new Catalog(bufferPoolManager);

        testTableSchema = new Schema(Arrays.asList(
                new Column("id", DataType.INT),
                new Column("name", DataType.VARCHAR)
        ));
        testTableInfo = catalog.createTable("test_table", testTableSchema);
    }

    @AfterEach
    void tearDown() throws IOException {
        diskManager.close();
        logManager.close();
        new File(TEST_DB_FILE_NAME).delete();
        new File(TEST_LOG_FILE_NAME).delete();
    }

    @Test
    void testSimpleInsertAndCommit() throws IOException {
        System.out.println("--- Test: Simple Insert and Commit ---");

        Transaction txn = transactionManager.begin();
        TableHeap tableHeap = new TableHeap(bufferPoolManager, testTableInfo, logManager, lockManager);
        Tuple tuple = new Tuple(Arrays.asList(new Value(101), new Value("Alice")));
        assertTrue(tableHeap.insertTuple(tuple, txn), "Tuple insertion should succeed");

        transactionManager.commit(txn);

        List<LogRecord> logs = readAllLogRecords(TEST_LOG_FILE_NAME, testTableSchema);
        assertEquals(3, logs.size());

        LogRecord beginLog = logs.get(0);
        assertEquals(LogRecord.LogType.BEGIN, beginLog.getLogType());

        LogRecord insertLog = logs.get(1);
        assertEquals(LogRecord.LogType.INSERT, insertLog.getLogType());
        assertEquals(beginLog.getLsn(), insertLog.getPrevLSN());

        LogRecord commitLog = logs.get(2);
        assertEquals(LogRecord.LogType.COMMIT, commitLog.getLogType());
        assertEquals(insertLog.getLsn(), commitLog.getPrevLSN());

        System.out.println("Log validation passed!");
    }

    @Test
    void testUpdateDeleteAndAbort() throws IOException {
        System.out.println("--- Test: Update, Delete and Abort ---");
        Transaction setupTxn = transactionManager.begin();
        TableHeap tableHeap = new TableHeap(bufferPoolManager, testTableInfo, logManager, lockManager);
        tableHeap.insertTuple(new Tuple(Arrays.asList(new Value(1), new Value("A"))), setupTxn);
        tableHeap.insertTuple(new Tuple(Arrays.asList(new Value(2), new Value("B"))), setupTxn);
        transactionManager.commit(setupTxn);

        Transaction txn = transactionManager.begin();
        Tuple newTuple1 = new Tuple(Arrays.asList(new Value(1), new Value("A_updated")));
        tableHeap.updateTuple(newTuple1, new RID(testTableInfo.getFirstPageId().getPageNum(), 0), txn);
        tableHeap.deleteTuple(new RID(testTableInfo.getFirstPageId().getPageNum(), 1), txn);
        transactionManager.abort(txn);

        List<LogRecord> logs = readAllLogRecords(TEST_LOG_FILE_NAME, testTableSchema);

        assertEquals(8, logs.size());
        System.out.println("Log validation for aborted transaction passed!");
    }

    private List<LogRecord> readAllLogRecords(String logFilePath, Schema schema) throws IOException {
        List<LogRecord> records = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(logFilePath, "r")) {
            long fileLength = file.length();
            long currentPosition = 0;

            while (currentPosition < fileLength) {
                file.seek(currentPosition);
                int recordSize = file.readInt();
                if (recordSize <= 0) break;

                byte[] recordBytes = new byte[recordSize];
                file.seek(currentPosition);
                int bytesRead = file.read(recordBytes);
                if (bytesRead != recordSize) {
                    System.err.println("Warning: Incomplete log record read.");
                    break;
                }

                ByteBuffer buffer = ByteBuffer.wrap(recordBytes);
                records.add(LogRecord.fromBytes(buffer, schema));
                currentPosition += recordSize;
            }
        }
        return records;
    }
}