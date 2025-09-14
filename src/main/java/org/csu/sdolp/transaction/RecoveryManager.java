package org.csu.sdolp.transaction;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecoveryManager {
    private final LogManager logManager;
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;
    private final LockManager lockManager; // Undo/Redo 操作也需要锁管理器

    public RecoveryManager(LogManager logManager, BufferPoolManager bufferPoolManager, Catalog catalog, LockManager lockManager) {
        this.logManager = logManager;
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
        this.lockManager = lockManager;
    }

    public void recover() throws IOException {
        System.out.println("[RecoveryManager] Starting recovery process...");
        List<LogRecord> allLogs = logManager.readAllLogRecords();

        if (allLogs.isEmpty()) {
            System.out.println("[RecoveryManager] Log file is empty. No recovery needed.");
            return;
        }

        // --- Phase 1: Analysis ---
        System.out.println("[RecoveryManager] --- Analysis Phase ---");
        Map<Integer, TransactionMetadata> activeTxnTable = new HashMap<>();
        for (LogRecord log : allLogs) {
            int txnId = log.getTransactionId();
            if (!activeTxnTable.containsKey(txnId)) {
                activeTxnTable.put(txnId, new TransactionMetadata(log.getLsn()));
            }
            TransactionMetadata txnMeta = activeTxnTable.get(txnId);
            txnMeta.lastLSN = log.getLsn();

            if (log.getLogType() == LogRecord.LogType.COMMIT || log.getLogType() == LogRecord.LogType.ABORT) {
                activeTxnTable.remove(txnId);
            }
        }
        System.out.println("[Analysis] Active transactions to be rolled back: " + activeTxnTable.keySet());

        // --- Phase 2: Redo ---
        System.out.println("[RecoveryManager] --- Redo Phase ---");
        for (LogRecord log : allLogs) {
            applyLog(log, false); // Redo阶段不需要记录新的日志
        }
        System.out.println("[Redo] All logged operations have been re-applied.");

        // --- Phase 3: Undo ---
        System.out.println("[RecoveryManager] --- Undo Phase ---");
        for (Integer txnId : activeTxnTable.keySet()) {
            System.out.println("[Undo] Rolling back transaction " + txnId);
            long lsnToUndo = activeTxnTable.get(txnId).lastLSN;

            while (lsnToUndo != -1) {
                LogRecord logToUndo = logManager.readLogRecord(lsnToUndo);
                if (logToUndo == null) break;

                if (logToUndo.getLogType() == LogRecord.LogType.CLR) {
                    lsnToUndo = logToUndo.getUndoNextLSN();
                } else {
                    // 1. 生成并写入补偿日志
                    LogRecord clr = generateCompensationLog(logToUndo);
                    logManager.appendLogRecord(clr);

                    // 2. 执行物理撤销
                    applyUndo(logToUndo);

                    lsnToUndo = logToUndo.getPrevLSN();
                }
            }
            // 3. 为被中止的事务写入一条 ABORT 日志
            Transaction fakeTxn = new Transaction(txnId);
            LogRecord abortLog = new LogRecord(fakeTxn.getTransactionId(), lsnToUndo, LogRecord.LogType.ABORT);
            logManager.appendLogRecord(abortLog);
            System.out.println("Transaction " + fakeTxn.getTransactionId() + " aborted.");
        }
        System.out.println("[RecoveryManager] Recovery process completed.");
    }

    /**
     * 根据日志记录，重做或撤销物理操作。
     */
    private void applyLog(LogRecord log, boolean isUndo) throws IOException {
        // DML 操作需要一个临时的事务对象
        Transaction fakeTxn = new Transaction(log.getTransactionId());


        switch (log.getLogType()) {
            // 类型 1: 事务控制日志，在 Redo/Undo 阶段无需物理操作，直接跳过
            case BEGIN:
            case COMMIT:
            case ABORT:
            case CLR:
                return;
            // 类型 2: DDL 日志
            case CREATE_TABLE:
                if (!isUndo && catalog.getTable(log.getTableName()) == null) {
                    catalog.createTable(log.getTableName(), log.getSchema());
                } else if (isUndo && catalog.getTable(log.getTableName()) != null) {
                    catalog.dropTable(log.getTableName());
                }
                return;
            case DROP_TABLE:
                if (!isUndo && catalog.getTable(log.getTableName()) != null) {
                    catalog.dropTable(log.getTableName());
                } else if (isUndo && catalog.getTable(log.getTableName()) == null) {
                    catalog.createTable(log.getTableName(), log.getSchema());
                } return;
            case ALTER_TABLE:
                if (!isUndo) {
                    catalog.addColumn(log.getTableName(), log.getNewColumn());
                } return;
            // 类型 3: DML 日志，现在可以安全地假设 log.getTableName() 不为 null
            case INSERT, DELETE, UPDATE:
                TableInfo tableInfo = catalog.getTable(log.getTableName());
                if (tableInfo == null) {
                    System.err.println("WARN: Table '" + log.getTableName() + "' not found for DML op during recovery, skipping LSN=" + log.getLsn());
                    return;
                }
                Schema schema = tableInfo.getSchema();
                TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo, logManager, lockManager);

                // 接下来是处理不同 DML 类型的逻辑
                if (log.getLogType() == LogRecord.LogType.INSERT) {
                    if (isUndo) {
                        tableHeap.deleteTuple(log.getRid(), fakeTxn, false);
                    } else {
                        Tuple tuple = Tuple.fromBytes(log.getTupleBytes(), schema);
                        tableHeap.insertTuple(tuple, fakeTxn, false, false);
                    }
                } else if (log.getLogType() == LogRecord.LogType.DELETE) {
                    if (isUndo) {
                        Tuple deletedTuple = Tuple.fromBytes(log.getTupleBytes(), schema);
                        deletedTuple.setRid(log.getRid());
                        tableHeap.insertTuple(deletedTuple, fakeTxn, false, false);
                    } else {
                        tableHeap.deleteTuple(log.getRid(), fakeTxn, false);
                    }
                } else if (log.getLogType() == LogRecord.LogType.UPDATE) {

                    Tuple oldTuple = Tuple.fromBytes(log.getOldTupleBytes(), schema);
                    Tuple newTuple = Tuple.fromBytes(log.getNewTupleBytes(), schema);
                    Tuple tupleToApply = isUndo ? oldTuple : newTuple;

                    // 核心修复点：调用 updateTuple，忽略返回值
                    tableHeap.updateTuple(tupleToApply, log.getRid(), fakeTxn, false);
                }
                break;
        }
    }

    private void applyUndo(LogRecord log) throws IOException {
        System.out.println("[Undo] Applying undo for LSN=" + log.getLsn() + ", Type=" + log.getLogType());
        applyLog(log, true); // applyLog 传入 isUndo=true 即可执行逆操作
    }

    private LogRecord generateCompensationLog(LogRecord logToUndo) {
        // CLR 记录了它要撤销的下一条日志的 LSN
        return new LogRecord(
                logToUndo.getTransactionId(),
                logToUndo.getPrevLSN(), // CLR的prevLSN继承自被撤销的日志
                LogRecord.LogType.CLR,
                logToUndo.getPrevLSN() // undoNextLSN 指向下一个要撤销的日志
        );
    }

    // 辅助内部类
    private static class TransactionMetadata {
        public long lastLSN;
        TransactionMetadata(long lsn) { this.lastLSN = lsn; }
    }
}