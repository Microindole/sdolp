package org.csu.sdolp.transaction.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LogManager {
    private final RandomAccessFile logFile;
    private final AtomicLong nextLSN;

    public LogManager(String logFilePath) throws IOException {
        File file = new File(logFilePath);
        this.logFile = new RandomAccessFile(file, "rw");
        long fileLength = logFile.length();
        this.nextLSN = new AtomicLong(fileLength);
    }

    /**
     * 将一条日志记录追加到日志文件并持久化到磁盘.
     * 这个方法必须是线程安全的.
     * @param logRecord 要写入的日志记录
     * @return 这条日志的序列号 (Log Sequence Number, LSN)
     */
    public synchronized long appendLogRecord(LogRecord logRecord) throws IOException {
        long currentLSN = nextLSN.get();
        logRecord.setLsn(currentLSN);

        byte[] recordBytes = logRecord.toBytes();

        logFile.seek(currentLSN);
        logFile.write(recordBytes);

        // **关键**：强制将日志刷入磁盘，确保持久性
        flush();

        // 更新下一条日志的 LSN
        nextLSN.addAndGet(recordBytes.length);

        System.out.println("[LogManager] Appended Log: LSN=" + currentLSN + ", Type=" + logRecord.getLogType() + ", TxnID=" + logRecord.getTransactionId());

        return currentLSN;
    }

    /**
     * 强制将日志缓冲区的所有内容写入磁盘。
     */
    public void flush() throws IOException {
        logFile.getFD().sync();
    }

    public void close() throws IOException {
        if (logFile != null) {
            logFile.close();
        }
    }

    /**
     * 从日志文件中读取并反序列化所有日志记录.
     * @return LogRecord列表
     */
    public List<LogRecord> readAllLogRecords() throws IOException {
        List<LogRecord> records = new ArrayList<>();
        long currentPosition = 0;
        long fileLength = logFile.length();

        while (currentPosition < fileLength) {
            logFile.seek(currentPosition);
            int recordSize = logFile.readInt();
            if (recordSize <= 0) break;

            byte[] recordBytes = new byte[recordSize];
            logFile.seek(currentPosition);
            int bytesRead = logFile.read(recordBytes);
            if (bytesRead != recordSize) break;

            ByteBuffer buffer = ByteBuffer.wrap(recordBytes);
            // 注意：恢复时我们不知道是哪个表的Schema，所以传null
            records.add(LogRecord.fromBytes(buffer, null));
            currentPosition += recordSize;
        }
        return records;
    }

    /**
     * 根据LSN（即文件偏移量）读取单条日志记录。
     * @param lsn 日志序列号
     * @return 读取到的LogRecord
     */
    public LogRecord readLogRecord(long lsn) throws IOException {
        logFile.seek(lsn);
        int recordSize = logFile.readInt();
        if (recordSize <= 0) return null;

        byte[] recordBytes = new byte[recordSize];
        logFile.seek(lsn);
        int bytesRead = logFile.read(recordBytes);
        if (bytesRead != recordSize) return null;

        ByteBuffer buffer = ByteBuffer.wrap(recordBytes);
        return LogRecord.fromBytes(buffer, null);
    }
}