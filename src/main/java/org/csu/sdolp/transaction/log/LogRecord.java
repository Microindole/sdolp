package org.csu.sdolp.transaction.log;

import lombok.Getter;
import lombok.Setter;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LogRecord {
    public enum LogType {
        INVALID, INSERT, DELETE, UPDATE, COMMIT, ABORT, BEGIN,
        CREATE_TABLE, DROP_TABLE, ALTER_TABLE
    }

    // --- Header ---
    private int recordSize = 0; // 整个记录的大小
    @Setter
    @Getter
    private long lsn = -1;      // 日志序列号
    @Getter
    private int transactionId;
    @Getter
    private long prevLSN = -1;  // 该事务的上一条日志的 LSN
    @Getter
    private LogType logType;

    // --- Payload for INSERT/DELETE ---
    @Getter
    private RID rid;
    @Getter
    private Tuple tuple; // INSERT: new tuple, DELETE: old tuple

    // --- Payload for UPDATE ---
    @Getter
    private Tuple oldTuple;
    @Getter
    private Tuple newTuple;

    // DDL 日志通常记录逻辑信息而非物理元组
    private String tableName;
    private Schema schema; // 用于 CREATE_TABLE
    private Column newColumn; // 用于 ALTER_TABLE

    // 构造函数 for INSERT/DELETE
    public LogRecord(int transactionId, long prevLSN, LogType logType, RID rid, Tuple tuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.rid = rid;
        this.tuple = tuple;
    }

    // 构造函数 for UPDATE
    public LogRecord(int transactionId, long prevLSN, LogType logType, RID rid, Tuple oldTuple, Tuple newTuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.rid = rid;
        this.oldTuple = oldTuple;
        this.newTuple = newTuple;
    }

    // 构造函数 for COMMIT/ABORT/BEGIN
    public LogRecord(int transactionId, long prevLSN, LogType logType) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
    }

    // 构造函数 for CREATE_TABLE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, Schema schema) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
        this.schema = schema;
    }

    // 构造函数 for DROP_TABLE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
    }

    // 构造函数 for ALTER_TABLE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, Column newColumn) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
        this.newColumn = newColumn;
    }

    // 私有构造函数，用于反序列化
    private LogRecord() {}


    public byte[] toBytes() {
        // 预计算 payload 大小
        int payloadSize = 0;
        byte[] tupleBytes = null;
        byte[] oldTupleBytes = null;
        byte[] newTupleBytes = null;

        if (logType == LogType.INSERT || logType == LogType.DELETE) {
            tupleBytes = tuple.toBytes();
            payloadSize = 8 + 4 + tupleBytes.length; // RID(8) + tuple_len(4) + tuple_data
        } else if (logType == LogType.UPDATE) {
            oldTupleBytes = oldTuple.toBytes();
            newTupleBytes = newTuple.toBytes();
            payloadSize = 8 + 4 + oldTupleBytes.length + 4 + newTupleBytes.length; // RID + old_len + old_data + new_len + new_data
        }

        recordSize = 28 + payloadSize; // 28 is the header size
        ByteBuffer buffer = ByteBuffer.allocate(recordSize);

        // --- 写入 Header ---
        buffer.putInt(recordSize);
        buffer.putLong(lsn);
        buffer.putInt(transactionId);
        buffer.putLong(prevLSN);
        buffer.putInt(logType.ordinal());

        // --- 写入 Payload ---
        if (logType == LogType.INSERT || logType == LogType.DELETE) {
            buffer.putInt(rid.pageNum());
            buffer.putInt(rid.slotIndex());
            buffer.putInt(tupleBytes.length);
            buffer.put(tupleBytes);
        } else if (logType == LogType.UPDATE) {
            buffer.putInt(rid.pageNum());
            buffer.putInt(rid.slotIndex());
            buffer.putInt(oldTupleBytes.length);
            buffer.put(oldTupleBytes);
            buffer.putInt(newTupleBytes.length);
            buffer.put(newTupleBytes);
        }

        return buffer.array();
    }

    /**
     * 从 ByteBuffer 反序列化 LogRecord.
     * @param buffer 包含日志数据的 ByteBuffer
     * @return 反序列化后的 LogRecord 对象
     */
    public static LogRecord fromBytes(ByteBuffer buffer, Schema schema) {
        LogRecord record = new LogRecord();
        record.recordSize = buffer.getInt();
        record.lsn = buffer.getLong();
        record.transactionId = buffer.getInt();
        record.prevLSN = buffer.getLong();
        record.logType = LogType.values()[buffer.getInt()];

        if (record.logType == LogType.INSERT || record.logType == LogType.DELETE) {
            int pageNum = buffer.getInt();
            int slotIndex = buffer.getInt();
            record.rid = new RID(pageNum, slotIndex);
            int tupleLen = buffer.getInt();
            byte[] tupleBytes = new byte[tupleLen];
            buffer.get(tupleBytes);
            // 注意：这里的反序列化需要 Schema，在简化场景下，我们先假设能拿到
            if (schema != null) {
                record.tuple = Tuple.fromBytes(tupleBytes, schema);
            }
        } else if (record.logType == LogType.UPDATE) {
            int pageNum = buffer.getInt();
            int slotIndex = buffer.getInt();
            record.rid = new RID(pageNum, slotIndex);

            if (schema != null) {
                int oldTupleLen = buffer.getInt();
                byte[] oldTupleBytes = new byte[oldTupleLen];
                buffer.get(oldTupleBytes);
                record.oldTuple = Tuple.fromBytes(oldTupleBytes, schema);

                int newTupleLen = buffer.getInt();
                byte[] newTupleBytes = new byte[newTupleLen];
                buffer.get(newTupleBytes);
                record.newTuple = Tuple.fromBytes(newTupleBytes, schema);
            }
        }
        return record;
    }

}