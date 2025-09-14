// src/main/java/org/csu/sdolp/executor/TableHeap.java
package org.csu.sdolp.executor;

import lombok.Getter;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.IOException;

public class TableHeap implements TupleIterator {

    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    @Getter
    private PageId firstPageId;
    private final LogManager logManager;
    @Getter
    private final LockManager lockManager;
    private final TableInfo tableInfo;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Page currentPage;
    private int currentSlotIndex;
    private Transaction iteratorTxn;

    public TableHeap(BufferPoolManager bufferPoolManager, TableInfo tableInfo, LogManager logManager, LockManager lockManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.tableInfo = tableInfo;
        this.schema = tableInfo.getSchema();
        this.firstPageId = tableInfo.getFirstPageId();
        this.logManager = logManager;
        this.lockManager = lockManager;
    }

    public void initIterator(Transaction txn) throws IOException {
        this.iteratorTxn = txn;
        this.currentPageId = this.firstPageId;
        this.currentSlotIndex = 0;
        try {
            if (this.currentPageId != null && this.currentPageId.getPageNum() != -1) {
                lockManager.lockShared(iteratorTxn, currentPageId);
                this.currentPage = bufferPoolManager.getPage(this.currentPageId);
            } else {
                this.currentPage = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }

    @Override
    public Schema getOutputSchema() {
        return this.schema;
    }

    @Override
    public Tuple next() throws IOException {
        if (!hasNext()) return null;
        Tuple tuple = currentPage.getTuple(currentSlotIndex, schema);
        if (tuple != null) {
            tuple.setRid(new RID(currentPageId.getPageNum(), currentSlotIndex));
        }
        currentSlotIndex++;
        return tuple;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (currentPage == null) return false;
        while (true) {
            if (currentSlotIndex < currentPage.getNumTuples()) {
                if (currentPage.getTuple(currentSlotIndex, schema) != null) {
                    return true;
                }
                currentSlotIndex++;
            } else {
                int nextPageNum = currentPage.getNextPageId();
                if (nextPageNum != -1) {
                    try {
                        PageId nextPid = new PageId(nextPageNum);
                        lockManager.lockShared(iteratorTxn, nextPid);
                        currentPageId = nextPid;
                        currentPage = bufferPoolManager.getPage(currentPageId);
                        currentSlotIndex = 0;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Thread interrupted while acquiring lock", e);
                    }
                } else {
                    return false;
                }
            }
        }
    }

    public boolean insertTuple(Tuple tuple, Transaction txn) throws IOException {
        // 正常插入总是需要加锁和写日志
        return insertTuple(tuple, txn, true, true);
    }

    // **公开给 RecoveryManager 的方法**
    public boolean insertTuple(Tuple tuple, Transaction txn, boolean acquireLock, boolean writeLog) throws IOException {
        try {
            Page targetPage = findFreePageForInsert(tuple, txn, acquireLock);
            if (targetPage == null) return false;

            int slotIndexOfNewTuple = targetPage.getNumTuples();
            if (!targetPage.insertTuple(tuple)) {
                return false;
            }
            RID rid = new RID(targetPage.getPageId().getPageNum(), slotIndexOfNewTuple);
            tuple.setRid(rid);

            if (writeLog) {
                LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.INSERT, this.tableInfo.getTableName(), rid, tuple);
                long lsn = logManager.appendLogRecord(logRecord);
                txn.setPrevLSN(lsn);
            }

            bufferPoolManager.flushPage(targetPage.getPageId());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }

    private Page findFreePageForInsert(Tuple tuple, Transaction txn, boolean acquireLock) throws IOException, InterruptedException {
        byte[] tupleBytes = tuple.toBytes();
        int requiredSpace = tupleBytes.length + 8;
        PageId pid = this.firstPageId;
        Page lastPage = null;
        while (pid != null && pid.getPageNum() != -1) {
            if (acquireLock) {
                lockManager.lockExclusive(txn, pid);
            }
            Page page = bufferPoolManager.getPage(pid);
            lastPage = page;
            if (page.getFreeSpace() >= requiredSpace) {
                return page;
            }
            int nextPageNum = page.getNextPageId();
            pid = (nextPageNum != -1) ? new PageId(nextPageNum) : null;
        }
        Page newPage = bufferPoolManager.newPage();
        if (newPage == null) return null;
        newPage.init();
        if (acquireLock) {
            lockManager.lockExclusive(txn, newPage.getPageId());
        }
        if (lastPage != null) {
            lastPage.setNextPageId(newPage.getPageId().getPageNum());
            bufferPoolManager.flushPage(lastPage.getPageId());
        } else {
            this.firstPageId = newPage.getPageId();
        }
        return newPage;
    }

    // --- 核心修改：为 Delete 和 Update 增加重载方法 ---

    // **给普通执行器（如DeleteExecutor）调用的公开方法**
    public boolean deleteTuple(RID rid, Transaction txn) throws IOException {
        // 正常操作总是需要加锁和写日志
        return deleteTuple(rid, txn, true, true);
    }

    // **给恢复管理器(RecoveryManager)调用的内部版本**
    public boolean deleteTuple(RID rid, Transaction txn, boolean acquireLock) throws IOException {
        // 恢复操作由RecoveryManager控制，不写新的DML日志
        return deleteTuple(rid, txn, acquireLock, false);
    }

    // **包含所有逻辑的私有核心方法**
    private boolean deleteTuple(RID rid, Transaction txn, boolean acquireLock, boolean writeLog) throws IOException {
        try {
            PageId pageId = new PageId(rid.pageNum());
            if (acquireLock) {
                lockManager.lockExclusive(txn, pageId);
            }
            Page page = bufferPoolManager.getPage(pageId);
            Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
            if (oldTuple == null) return false;

            if (writeLog) {
                LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.DELETE, this.tableInfo.getTableName(), rid, oldTuple);
                long lsn = logManager.appendLogRecord(logRecord);
                txn.setPrevLSN(lsn);
            }

            boolean success = page.deleteTuple(rid.slotIndex());
            if (success) {
                bufferPoolManager.flushPage(page.getPageId());
            }
            return success;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }


    // **给普通执行器（如UpdateExecutor）调用的公开方法**
    public boolean updateTuple(Tuple newTuple, RID rid, Transaction txn) throws IOException {
        // 正常操作总是需要加锁和写日志
        return updateTuple(newTuple, rid, txn, true, true);
    }

    // **给恢复管理器(RecoveryManager)调用的内部版本**
    public boolean updateTuple(Tuple newTuple, RID rid, Transaction txn, boolean acquireLock) throws IOException {
        // 恢复操作由RecoveryManager控制，不写新的DML日志
        return updateTuple(newTuple, rid, txn, acquireLock, false);
    }

    // **包含所有逻辑的私有核心方法**
    private boolean updateTuple(Tuple newTuple, RID rid, Transaction txn, boolean acquireLock, boolean writeLog) throws IOException {
        try {
            PageId pageId = new PageId(rid.pageNum());
            if (acquireLock) {
                lockManager.lockExclusive(txn, pageId);
            }
            Page page = bufferPoolManager.getPage(pageId);
            Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
            if (oldTuple == null) return false;

            if (writeLog) {
                LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.UPDATE, this.tableInfo.getTableName(), rid, oldTuple, newTuple);
                long lsn = logManager.appendLogRecord(logRecord);
                txn.setPrevLSN(lsn);
            }

            if (page.deleteTuple(rid.slotIndex())) { // 调用物理删除方法
                // 更新是先删除再插入
                int newSlotIndex = page.getNumTuples();
                if(page.insertTuple(newTuple)){
                    newTuple.setRid(new RID(pageId.getPageNum(), newSlotIndex));
                    bufferPoolManager.flushPage(page.getPageId());
                    return true;
                }

                // 如果插入失败，这里无法“撤销删除”
                System.err.println("Update failed: Not enough space on page after physical delete.");
                // 在这种情况下，旧数据已被删除，需要一个更复杂的恢复机制。
                // 对于当前简化模型，我们可以返回失败。
                return false;
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }

    public Tuple getTuple(RID rid, Transaction txn) throws IOException {
        try {
            PageId pageId = new PageId(rid.pageNum());
            // 读取数据需要加共享锁
            lockManager.lockShared(txn, pageId);
            Page page = bufferPoolManager.getPage(pageId);
            Tuple tuple = page.getTuple(rid.slotIndex(), schema);
            if (tuple != null) {
                tuple.setRid(rid);
            }
            // 注意：事务结束前不应释放锁，这里只是获取页面
            return tuple;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock for getTuple", e);
        }
    }
}