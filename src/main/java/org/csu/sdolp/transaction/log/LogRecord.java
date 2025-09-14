// src/main/java/org/csu/sdolp/transaction/log/LogRecord.java

package org.csu.sdolp.transaction.log;

import lombok.Getter;
import lombok.Setter;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Getter
public class LogRecord {
    public enum LogType {
        INVALID, INSERT, DELETE, UPDATE, COMMIT, ABORT, BEGIN,
        CREATE_TABLE, DROP_TABLE, ALTER_TABLE,
        CLR
    }

    // --- Header ---
    private int recordSize = 0;
    @Setter
    private long lsn = -1;
    private int transactionId;
    private long prevLSN = -1;
    private LogType logType;

    // private Tuple tuple;
    // private Tuple oldTuple;
    // private Tuple newTuple;

    // --- Payload for INSERT/DELETE ---
    private RID rid;
    private byte[] tupleBytes; // INSERT: new tuple bytes, DELETE: old tuple bytes

    // --- Payload for UPDATE ---
    private byte[] oldTupleBytes;
    private byte[] newTupleBytes;

    // --- Payload for CLR ---
    private long undoNextLSN;

    // DDL 日志字段
    private String tableName;
    private Schema schema;
    private Column newColumn;

    // 构造函数 for INSERT/DELETE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, RID rid, Tuple tuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
        this.rid = rid;
        // 修复点：在构造时立即序列化
        this.tupleBytes = tuple.toBytes();
    }

    // 构造函数 for UPDATE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, RID rid, Tuple oldTuple, Tuple newTuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
        this.rid = rid;
        // 修复点：在构造时立即序列化
        this.oldTupleBytes = oldTuple.toBytes();
        this.newTupleBytes = newTuple.toBytes();
    }

    // --- 其他构造函数保持不变 ---

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

    // 构造函数 for CLR (补偿日志)
    public LogRecord(int transactionId, long prevLSN, LogType logType, long undoNextLSN) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.undoNextLSN = undoNextLSN;
    }

    // 私有构造函数，用于反序列化
    private LogRecord() {}

    public byte[] toBytes() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            dos.writeLong(lsn);
            dos.writeInt(transactionId);
            dos.writeLong(prevLSN);
            dos.writeInt(logType.ordinal());

            switch (logType) {
                case INSERT, DELETE -> {
                    dos.writeUTF(tableName);
                    dos.writeInt(rid.pageNum());
                    dos.writeInt(rid.slotIndex());
                    // 修复点：直接写入已序列化的字节
                    dos.writeInt(tupleBytes.length);
                    dos.write(tupleBytes);
                }
                case UPDATE -> {
                    dos.writeUTF(tableName);
                    dos.writeInt(rid.pageNum());
                    dos.writeInt(rid.slotIndex());
                    // 修复点：直接写入已序列化的字节
                    dos.writeInt(oldTupleBytes.length);
                    dos.write(oldTupleBytes);
                    dos.writeInt(newTupleBytes.length);
                    dos.write(newTupleBytes);
                }
                case CREATE_TABLE -> {
                    dos.writeUTF(tableName);
                    schema.write(dos);
                }
                case DROP_TABLE -> dos.writeUTF(tableName);
                case ALTER_TABLE -> {
                    dos.writeUTF(tableName);
                    newColumn.write(dos);
                }
                case CLR -> dos.writeLong(undoNextLSN);
            }
            dos.close();

            byte[] recordData = baos.toByteArray();
            ByteBuffer buffer = ByteBuffer.allocate(4 + recordData.length);
            buffer.putInt(4 + recordData.length);
            buffer.put(recordData);
            return buffer.array();
        } catch (IOException e) {
            throw new RuntimeException("LogRecord serialization failed", e);
        }
    }

    public static LogRecord fromBytes(ByteBuffer buffer, Schema tableSchema) {
        int recordSize = buffer.getInt();
        // 健壮性修复：检查 recordSize，防止读取越界
        if (recordSize - 4 > buffer.remaining()) {
            throw new RuntimeException("LogRecord deserialization failed: invalid record size.");
        }
        byte[] recordData = new byte[recordSize - 4];
        buffer.get(recordData);

        ByteArrayInputStream bais = new ByteArrayInputStream(recordData);
        DataInputStream dis = new DataInputStream(bais);
        LogRecord record = new LogRecord();

        try {
            record.lsn = dis.readLong();
            record.transactionId = dis.readInt();
            record.prevLSN = dis.readLong();
            record.logType = LogType.values()[dis.readInt()];

            switch (record.logType) {
                case INSERT, DELETE -> {
                    record.tableName = dis.readUTF();
                    record.rid = new RID(dis.readInt(), dis.readInt());
                    int tupleLen = dis.readInt();
                    record.tupleBytes = new byte[tupleLen];
                    dis.readFully(record.tupleBytes);
                    // 反序列化时不再需要创建 Tuple 对象
                }
                case UPDATE -> {
                    record.tableName = dis.readUTF();
                    record.rid = new RID(dis.readInt(), dis.readInt());
                    int oldTupleLen = dis.readInt();
                    record.oldTupleBytes = new byte[oldTupleLen];
                    dis.readFully(record.oldTupleBytes);

                    int newTupleLen = dis.readInt();
                    record.newTupleBytes = new byte[newTupleLen];
                    dis.readFully(record.newTupleBytes);
                    // 反序列化时不再需要创建 Tuple 对象
                }
                case CREATE_TABLE -> {
                    record.tableName = dis.readUTF();
                    record.schema = Schema.read(dis);
                }
                case DROP_TABLE -> record.tableName = dis.readUTF();
                case ALTER_TABLE -> {
                    record.tableName = dis.readUTF();
                    record.newColumn = Column.read(dis);
                }
                case CLR -> record.undoNextLSN = dis.readLong();
            }
            dis.close();
        } catch (IOException e) {
            throw new RuntimeException("LogRecord deserialization failed", e);
        }
        return record;
    }
}