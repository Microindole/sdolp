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
    private int recordSize = 0; // 整个记录的大小
    @Setter
    private long lsn = -1;      // 日志序列号
    private int transactionId;
    private long prevLSN = -1;  // 该事务的上一条日志的 LSN
    private LogType logType;

    // --- Payload for INSERT/DELETE ---
    private RID rid;
    private Tuple tuple; // INSERT: new tuple, DELETE: old tuple

    // --- Payload for UPDATE ---
    private Tuple oldTuple;
    private Tuple newTuple;

    // --- Payload for CLR ---
    private long undoNextLSN;

    // 这些字段在恢复模式下（Schema为null时）被填充
    private byte[] tupleBytes;
    private byte[] oldTupleBytes;
    private byte[] newTupleBytes;

    // DDL 日志通常记录逻辑信息而非物理元组
    private String tableName;
    private Schema schema; // 用于 CREATE_TABLE
    private Column newColumn; // 用于 ALTER_TABLE

    // 构造函数 for INSERT/DELETE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, RID rid, Tuple tuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
        this.rid = rid;
        this.tuple = tuple;
    }

    // 构造函数 for UPDATE
    public LogRecord(int transactionId, long prevLSN, LogType logType, String tableName, RID rid, Tuple oldTuple, Tuple newTuple) {
        this.transactionId = transactionId;
        this.prevLSN = prevLSN;
        this.logType = logType;
        this.tableName = tableName;
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
                    byte[] tupleData = tuple.toBytes();
                    dos.writeInt(tupleData.length);
                    dos.write(tupleData);
                }
                case UPDATE -> {
                    dos.writeUTF(tableName);
                    dos.writeInt(rid.pageNum());
                    dos.writeInt(rid.slotIndex());
                    byte[] oldTupleData = oldTuple.toBytes();
                    dos.writeInt(oldTupleData.length);
                    dos.write(oldTupleData);
                    byte[] newTupleData = newTuple.toBytes();
                    dos.writeInt(newTupleData.length);
                    dos.write(newTupleData);
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
                    // *** 核心修复: 无论 schema 是否为 null，都保存原始字节 ***
                    record.tupleBytes = new byte[tupleLen];
                    dis.readFully(record.tupleBytes);
                    if (tableSchema != null) {
                        record.tuple = Tuple.fromBytes(record.tupleBytes, tableSchema);
                    }
                }
                case UPDATE -> {
                    record.tableName = dis.readUTF();
                    record.rid = new RID(dis.readInt(), dis.readInt());
                    int oldTupleLen = dis.readInt();
                    // *** 核心修复: 保存 oldTuple 的原始字节 ***
                    record.oldTupleBytes = new byte[oldTupleLen];
                    dis.readFully(record.oldTupleBytes);

                    int newTupleLen = dis.readInt();
                    // *** 核心修复: 保存 newTuple 的原始字节 ***
                    record.newTupleBytes = new byte[newTupleLen];
                    dis.readFully(record.newTupleBytes);

                    if (tableSchema != null) {
                        record.oldTuple = Tuple.fromBytes(record.oldTupleBytes, tableSchema);
                        record.newTuple = Tuple.fromBytes(record.newTupleBytes, tableSchema);
                    }
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