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

/**
 * TableHeap 负责一张表的物理存储堆，并提供遍历其所有元组的能力。
 * 它封装了页面链表的迭代、元组的插入、删除和更新等底层操作。
 * 这是所有上层执行器的物理数据源。
 */
public class TableHeap implements TupleIterator {

    private final BufferPoolManager bufferPoolManager;
    private final Schema schema;
    @Getter
    private PageId firstPageId;
    private final LogManager logManager;
    @Getter
    private final LockManager lockManager;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Page currentPage;
    private int currentSlotIndex;
    private Transaction iteratorTxn;

    public TableHeap(BufferPoolManager bufferPoolManager, TableInfo tableInfo, LogManager logManager, LockManager lockManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = tableInfo.getSchema();
        this.firstPageId = tableInfo.getFirstPageId();
        this.logManager = logManager;
        this.lockManager = lockManager; // *** 初始化 ***
    }

    public void initIterator(Transaction txn) throws IOException {
        this.iteratorTxn = txn;
        this.currentPageId = this.firstPageId;

        try {
            if (this.currentPageId != null && this.currentPageId.getPageNum() != -1) {
                // *** 加锁点 1：为读操作获取共享锁 ***
                lockManager.lockShared(iteratorTxn, currentPageId);
                this.currentPage = bufferPoolManager.getPage(this.currentPageId);
            } else {
                this.currentPage = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
        this.currentSlotIndex = 0;
    }

    @Override
    public Schema getOutputSchema() {
        return this.schema;
    }

    // next() 和 hasNext() 方法保持不变，它们是只读操作
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
                        // *** 加锁点 2：切换到下一页时，为新页面加共享锁 ***
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

    /**
     * 向表中插入一条元组，并遵循WAL协议。
     * @param tuple 要插入的元组
     * @param txn   当前事务
     * @return 插入成功返回 true
     */
    public boolean insertTuple(Tuple tuple, Transaction txn) throws IOException {


        try {
            Page targetPage = findFreePageForInsert(tuple, txn);
            if (targetPage == null) return false;
                RID rid = new RID(targetPage.getPageId().getPageNum(), targetPage.getNumTuples());
                tuple.setRid(rid);

                LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.INSERT, rid, tuple);
                long lsn = logManager.appendLogRecord(logRecord);
                txn.setPrevLSN(lsn);

                boolean success = targetPage.insertTuple(tuple);
                if (success) {
                    bufferPoolManager.flushPage(targetPage.getPageId());
                }
                return success;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Thread interrupted while acquiring lock", e);
            }
    }

    private Page findFreePageForInsert(Tuple tuple, Transaction txn) throws IOException, InterruptedException {
        byte[] tupleBytes = tuple.toBytes();
        int requiredSpace = tupleBytes.length + 8;

        PageId pid = this.firstPageId;
        Page lastPage = null;

        while (pid != null && pid.getPageNum() != -1) {
            lockManager.lockExclusive(txn, pid);
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

        lockManager.lockExclusive(txn, newPage.getPageId());

        if (lastPage != null) {
            lastPage.setNextPageId(newPage.getPageId().getPageNum());
            bufferPoolManager.flushPage(lastPage.getPageId());
        } else {
            this.firstPageId = newPage.getPageId();
        }
        return newPage;
    }

    public boolean deleteTuple(RID rid, Transaction txn, boolean acquireLock) throws IOException {
        try {
            PageId pageId = new PageId(rid.pageNum());
            // *** 加锁点 4：为写操作获取排他锁 ***
            if (acquireLock) {
                lockManager.lockExclusive(txn, pageId);
            }
            Page page = bufferPoolManager.getPage(pageId);
            Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
            if (oldTuple == null) return false;
            LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.DELETE, rid, oldTuple);
            long lsn = logManager.appendLogRecord(logRecord);
            txn.setPrevLSN(lsn);
            boolean success = page.markTupleAsDeleted(rid.slotIndex());
            if (success) {
                bufferPoolManager.flushPage(page.getPageId());
            }
            return success;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }

    public boolean updateTuple(Tuple newTuple, RID rid, Transaction txn) throws IOException {
        return updateTuple(newTuple, rid, txn, true); // 默认行为是加锁
    }

    public boolean deleteTuple(RID rid, Transaction txn) throws IOException {
        return deleteTuple(rid, txn, true);
    }

    public boolean updateTuple(Tuple newTuple, RID rid, Transaction txn, boolean acquireLock) throws IOException {
        try {
            PageId pageId = new PageId(rid.pageNum());
            // *** 加锁点 5：为写操作获取排他锁 ***
            if (acquireLock) {
                lockManager.lockExclusive(txn, pageId);
            }
            Page page = bufferPoolManager.getPage(pageId);
            Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
            if (oldTuple == null) return false;

            LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.UPDATE, rid, oldTuple, newTuple);
            long lsn = logManager.appendLogRecord(logRecord);
            txn.setPrevLSN(lsn);

            if (page.markTupleAsDeleted(rid.slotIndex())) {
                if (page.getFreeSpace() >= newTuple.toBytes().length + 8) {
                    if (page.insertTuple(newTuple)) {
                        bufferPoolManager.flushPage(page.getPageId());
                        return true;
                    }
                }
                System.err.println("Update failed: Not enough space on page for in-place update.");
                return false;
            }

            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring lock", e);
        }
    }


    /**
     * 根据RID删除一条元组，并遵循WAL协议。
     * @param rid 要删除元组的物理地址
     * @param txn 当前事务
     * @return 删除成功返回 true
     */
    public boolean deleteTuple(Tuple newTuple, RID rid, Transaction txn) throws IOException {
        Page page = bufferPoolManager.getPage(new PageId(rid.pageNum()));
        Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
        if (oldTuple == null) {
            return false; // 元组不存在或已被删除
        }

        // 1.【WAL核心】在修改数据页之前，先写入日志记录
        LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.DELETE, rid, oldTuple);
        long lsn = logManager.appendLogRecord(logRecord);
        txn.setPrevLSN(lsn);

        // 2.【物理修改】日志写入成功后，再标记元组为已删除
        boolean success = page.markTupleAsDeleted(rid.slotIndex());
        if (success) {
            bufferPoolManager.flushPage(page.getPageId());
        }
        return success;
    }


}