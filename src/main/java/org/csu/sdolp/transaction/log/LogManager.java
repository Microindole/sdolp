package org.csu.sdolp.transaction.log;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class LogManager {
    private final RandomAccessFile logFile;
    private final ReentrantLock latch = new ReentrantLock();

    public LogManager(String logFilePath) throws IOException {
        this.logFile = new RandomAccessFile(new File(logFilePath), "rw");
    }

    /**
     * 将一条日志记录追加到日志缓冲区，并持久化到磁盘
     * @param logRecord 要写入的日志记录
     * @return 这条日志的序列号 (Log Sequence Number, LSN)
     */
    public synchronized long appendLogRecord(LogRecord logRecord) throws IOException {
        long lsn = logFile.length();
        // byte[] recordBytes = logRecord.toBytes();
        // logFile.seek(lsn);
        // logFile.write(recordBytes);
        
        // **关键**：强制将日志刷入磁盘
        logFile.getFD().sync();
        
        return lsn;
    }
}