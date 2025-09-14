package org.csu.sdolp.cli.tool;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * 一个用于读取和分析指定数据库日志文件 (.log) 的工具。
 * 这个版本更智能，会提示用户从现有数据库列表中选择一个，并能解析DML日志内容。
 */
public class LogReader {

    public static void main(String[] args) throws IOException {
        DatabaseManager dbManager = new DatabaseManager();
        List<String> databases = dbManager.listDatabases();

        if (databases.isEmpty()) {
            System.out.println("在 'data' 目录下没有找到任何数据库。");
            return;
        }

        System.out.println("--- MiniDB 日志文件读取器 ---");
        System.out.println("可用的数据库: " + databases);

        Scanner scanner = new Scanner(System.in);
        String dbName = "";
        boolean isValidDb = false;

        while (!isValidDb) {
            System.out.print("请输入要读取日志的数据库名称: ");
            dbName = scanner.nextLine();
            if (databases.contains(dbName)) {
                isValidDb = true;
            } else {
                System.err.println("错误: 数据库 '" + dbName + "' 不存在，请重试。");
            }
        }
        scanner.close();

        String dbFilePath = DatabaseManager.getDbFilePath(dbName);
        String logFilePath = dbFilePath + ".log";

        System.out.println("正在分析数据库: " + dbName + " (日志文件: " + logFilePath + ")");

        File logFile = new File(logFilePath);
        if (!logFile.exists()) {
            System.out.println("日志文件 " + logFilePath + " 不存在。");
            return;
        }

        // 为了解析Tuple，我们需要Catalog来获取表的Schema
        DiskManager diskManager = new DiskManager(dbFilePath);
        diskManager.open();
        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager, "LRU");
        Catalog catalog = new Catalog(bufferPoolManager);

        List<LogRecord> records = readAllLogRecords(logFilePath);

        System.out.println("\n--- 解析出的日志记录 (" + records.size() + "条) ---");
        for (LogRecord record : records) {
            System.out.println(formatLogRecord(record, catalog));
        }

        diskManager.close();
    }

    private static String formatLogRecord(LogRecord record, Catalog catalog) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("LSN: %-5d | TxnID: %-3d | PrevLSN: %-5d | Type: %-12s |",
                record.getLsn(), record.getTransactionId(), record.getPrevLSN(), record.getLogType()));

        Schema schema = null;
        if (record.getTableName() != null && catalog.getTable(record.getTableName()) != null) {
            schema = catalog.getTable(record.getTableName()).getSchema();
        }

        switch (record.getLogType()) {
            case INSERT:
                if (schema != null) {
                    Tuple inserted = Tuple.fromBytes(record.getTupleBytes(), schema);
                    sb.append(" Table: ").append(record.getTableName()).append(" | RID: ").append(record.getRid()).append(" | Tuple: ").append(inserted);
                }
                break;
            case DELETE:
                if (schema != null) {
                    Tuple deleted = Tuple.fromBytes(record.getTupleBytes(), schema);
                    sb.append(" Table: ").append(record.getTableName()).append(" | RID: ").append(record.getRid()).append(" | Tuple: ").append(deleted);
                }
                break;
            case UPDATE:
                if (schema != null) {
                    Tuple oldTuple = Tuple.fromBytes(record.getOldTupleBytes(), schema);
                    Tuple newTuple = Tuple.fromBytes(record.getNewTupleBytes(), schema);
                    sb.append(" Table: ").append(record.getTableName()).append(" | RID: ").append(record.getRid())
                            .append(" | Old: ").append(oldTuple).append(" | New: ").append(newTuple);
                }
                break;
            case CREATE_TABLE:
                sb.append(" Table: ").append(record.getTableName());
                break;
            case CLR:
                sb.append(" UndoNextLSN: ").append(record.getUndoNextLSN());
                break;
            default:
                // For BEGIN, COMMIT, ABORT, etc., no extra info needed.
                break;
        }
        return sb.toString();
    }

    private static List<LogRecord> readAllLogRecords(String logFilePath) throws IOException {
        List<LogRecord> records = new ArrayList<>();
        try (RandomAccessFile file = new RandomAccessFile(logFilePath, "r")) {
            long fileLength = file.length();
            long currentPosition = 0;

            while (currentPosition < fileLength) {
                file.seek(currentPosition);
                if (file.length() - currentPosition < 4) break;
                int recordSize = file.readInt();
                if (recordSize <= 0 || recordSize > file.length() - currentPosition) {
                    System.err.println("警告: 发现无效的日志记录大小(" + recordSize + ")在偏移量 " + currentPosition + "。停止解析。");
                    break;
                }

                byte[] recordBytes = new byte[recordSize];
                file.seek(currentPosition);
                int bytesRead = file.read(recordBytes);
                if (bytesRead != recordSize) {
                    System.err.println("警告: 读取到不完整的日志记录。");
                    break;
                }

                ByteBuffer buffer = ByteBuffer.wrap(recordBytes);
                records.add(LogRecord.fromBytes(buffer, null));
                currentPosition += recordSize;
            }
        }
        return records;
    }
}