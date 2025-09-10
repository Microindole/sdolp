package org.csu.sdolp.transaction;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
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
                    long clrLsn = logManager.appendLogRecord(clr);

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

        // DDL 操作直接作用于 Catalog
        switch (log.getLogType()) {
            case CREATE_TABLE:
                if (!isUndo && catalog.getTable(log.getTableName()) == null) {
                    catalog.createTable(log.getTableName(), log.getSchema());
                } else if (isUndo && catalog.getTable(log.getTableName()) != null) {
                    catalog.dropTable(log.getTableName());
                } return;
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
        }

        // DML 操作
        TableInfo tableInfo = catalog.getTableByTuple(log.getTuple() != null ? log.getTuple() : log.getOldTuple());
        if (tableInfo == null) {
            System.err.println("WARN: Table for tuple not found during recovery, skipping log LSN=" + log.getLsn());
            return;
        }
        TableHeap tableHeap = new TableHeap(bufferPoolManager, tableInfo, logManager, lockManager);

        switch (log.getLogType()) {
            case INSERT:
                // Redo: 插入元组. Undo: 删除元组.
                if (!isUndo) tableHeap.insertTuple(log.getTuple(), fakeTxn, false);
                else tableHeap.deleteTuple(log.getRid(), fakeTxn, false);
                break;
            case DELETE:
                // Redo: 删除元组. Undo: 插入被删除的元组.
                if (!isUndo) tableHeap.deleteTuple(log.getRid(), fakeTxn, false);
                else tableHeap.insertTuple(log.getTuple(), fakeTxn, false);
                break;
            case UPDATE:
                // Redo: 更新为新元组. Undo: 更新回旧元组.
                Tuple tupleToApply = isUndo ? log.getOldTuple() : log.getNewTuple();
                tableHeap.updateTuple(tupleToApply, log.getRid(), fakeTxn, false);
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