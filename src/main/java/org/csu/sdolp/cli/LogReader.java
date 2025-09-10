package org.csu.sdolp.cli;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 一个用于读取和分析 minidb.data.log 文件的工具。
 */
public class LogReader {

    public static void main(String[] args) throws IOException {
        final String LOG_FILE_PATH = "minidb.data.log";
        System.out.println("--- MiniDB 日志文件读取器 ---");

        File logFile = new File(LOG_FILE_PATH);
        if (!logFile.exists()) {
            System.out.println("日志文件 " + LOG_FILE_PATH + " 不存在。");
            return;
        }

        System.out.println("正在分析文件: " + LOG_FILE_PATH);
        List<LogRecord> records = readAllLogRecords(LOG_FILE_PATH, null); // Schema为null，因为我们只关心日志头

        System.out.println("\n--- 解析出的日志记录 (" + records.size() + "条) ---");
        for (LogRecord record : records) {
            System.out.printf("LSN: %-5d | TxnID: %-3d | PrevLSN: %-5d | Type: %-15s\n",
                    record.getLsn(),
                    record.getTransactionId(),
                    record.getPrevLSN(),
                    record.getLogType());
        }
    }

    /**
     * 从日志文件中读取并反序列化所有日志记录.
     * @param logFilePath 日志文件路径
     * @param schema 表的Schema，用于反序列化Tuple (此处简化，可传入null)
     * @return LogRecord列表
     */
    private static List<LogRecord> readAllLogRecords(String logFilePath, Schema schema) throws IOException {
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
                // 注意：fromBytes目前只能解析DML日志的payload，对于DDL会跳过
                // 但对于我们的测试目的，只看日志头已经足够了
                records.add(LogRecord.fromBytes(buffer, schema));
                currentPosition += recordSize;
            }
        }
        return records;
    }
}