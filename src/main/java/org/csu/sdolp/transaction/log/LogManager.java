package org.csu.sdolp.transaction.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
}