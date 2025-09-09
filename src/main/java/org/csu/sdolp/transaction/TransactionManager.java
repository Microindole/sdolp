package org.csu.sdolp.transaction;

import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.log.LogManager;

import java.io.IOException;

public class TransactionManager {
    private final LockManager lockManager;
    private final LogManager logManager;

    public TransactionManager(LockManager lockManager, LogManager logManager) {
        this.lockManager = lockManager;
        this.logManager = logManager;
    }

    public Transaction begin() {
        Transaction txn = new Transaction();
        // 可以选择在这里写一条 BEGIN 日志
        // logManager.appendLogRecord(new LogRecord(txn.getId(), LogType.BEGIN));
        System.out.println("Transaction " + txn.getTransactionId() + " started.");
        return txn;
    }

    public void commit(Transaction txn) throws IOException {
        // 1. 写入 COMMIT 日志并刷盘
        // logManager.appendLogRecord(new LogRecord(txn.getId(), LogType.COMMIT));
        
        // 2. 释放该事务所持有的所有锁
        for (Integer pageIdNum : txn.getLockedPageIds()) {
            lockManager.unlock(txn, new PageId(pageIdNum));
        }
        txn.setState(Transaction.State.COMMITTED);
        System.out.println("Transaction " + txn.getTransactionId() + " committed.");
    }

    public void abort(Transaction txn) {
        System.out.println("Transaction " + txn.getTransactionId() + " is aborting.");
        // 1. **回滚操作**：这是一个复杂的过程。你需要反向遍历该事务的日志链(通过PrevLSN)，
        //    对每一个INSERT/DELETE/UPDATE操作，执行其逆操作，并写入一条补偿日志记录(CLR)。
        
        // 2. 写入 ABORT 日志
        // logManager.appendLogRecord(new LogRecord(txn.getId(), LogType.ABORT));

        // 3. 释放所有锁
        for (Integer pageIdNum : txn.getLockedPageIds()) {
            lockManager.unlock(txn, new PageId(pageIdNum));
        }
        txn.setState(Transaction.State.ABORTED);
        System.out.println("Transaction " + txn.getTransactionId() + " aborted.");
    }
}