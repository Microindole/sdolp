package org.csu.sdolp.transaction;

import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.IOException;
import java.util.ArrayList;

public class TransactionManager {
    private final LockManager lockManager;
    private final LogManager logManager;

    public TransactionManager(LockManager lockManager, LogManager logManager) {
        this.lockManager = lockManager;
        this.logManager = logManager;
    }

    public Transaction begin() throws IOException {
        Transaction txn = new Transaction();
        // --- 写入 BEGIN 日志 ---
        LogRecord beginLog = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.BEGIN);
        long lsn = logManager.appendLogRecord(beginLog);
        txn.setPrevLSN(lsn); // 更新事务的LSN链头

        System.out.println("Transaction " + txn.getTransactionId() + " started.");
        return txn;
    }

    public void commit(Transaction txn) throws IOException {
        // --- 写入 COMMIT 日志并刷盘 ---
        LogRecord commitLog = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.COMMIT);
        long lsn = logManager.appendLogRecord(commitLog);
        txn.setPrevLSN(lsn);

        // 确保所有之前的日志都已落盘
        logManager.flush();

        // 释放该事务所持有的所有锁
        for (Integer pageIdNum : new ArrayList<>(txn.getLockedPageIds())) {
            lockManager.unlock(txn, new PageId(pageIdNum));
        }

        txn.setState(Transaction.State.COMMITTED);
        System.out.println("Transaction " + txn.getTransactionId() + " committed.");
    }

    public void abort(Transaction txn) throws IOException {
        System.out.println("Transaction " + txn.getTransactionId() + " is aborting.");
        // 1. **回滚操作**：(此部分在恢复(Recovery)阶段实现，此处省略)
        //    你需要反向遍历该事务的日志链(通过prevLSN)，
        //    对每一个INSERT/DELETE/UPDATE操作，执行其逆操作，并写入一条补偿日志记录(CLR)。

        // --- 写入 ABORT 日志 ---
        LogRecord abortLog = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.ABORT);
        long lsn = logManager.appendLogRecord(abortLog);
        txn.setPrevLSN(lsn);

        // 释放所有锁
        for (Integer pageIdNum : new ArrayList<>(txn.getLockedPageIds())) {
            lockManager.unlock(txn, new PageId(pageIdNum));
        }
        txn.setState(Transaction.State.ABORTED);
        System.out.println("Transaction " + txn.getTransactionId() + " aborted.");
    }
}