package org.csu.sdolp.transaction.log;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Tuple;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class LogRecord {
    public enum LogType {
        INSERT, DELETE, UPDATE, COMMIT, ABORT, BEGIN
    }

    private final LogType logType;
    private final int transactionId;
    private long prevLSN = -1; // 上一条日志的 LSN

    // for INSERT/DELETE
    private RID rid;
    private Tuple tuple;

    // for UPDATE
    private Tuple oldTuple;
    private Tuple newTuple;

    // 构造函数 for INSERT/DELETE
    public LogRecord(LogType logType, int transactionId, RID rid, Tuple tuple) {
        this.logType = logType;
        this.transactionId = transactionId;
        this.rid = rid;
        this.tuple = tuple;
    }

    // 构造函数 for UPDATE
    public LogRecord(LogType logType, int transactionId, RID rid, Tuple oldTuple, Tuple newTuple) {
        this.logType = logType;
        this.transactionId = transactionId;
        this.rid = rid;
        this.oldTuple = oldTuple;
        this.newTuple = newTuple;
    }

    // 构造函数 for COMMIT/ABORT/BEGIN
    public LogRecord(LogType logType, int transactionId) {
        this.logType = logType;
        this.transactionId = transactionId;
    }

    public byte[] toBytes() {
        // 简化版序列化: [size | LogType | txnId | prevLSN | ...payload... ]
        // 实际实现会更复杂
        // 这里暂时返回一个空数组，重点在于API设计
        return new byte[0];
    }

    public static LogRecord fromBytes(byte[] data) {
        // 简化版反序列化
        return null;
    }
}