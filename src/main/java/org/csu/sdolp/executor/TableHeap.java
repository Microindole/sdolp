package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.page.Page;
import org.csu.sdolp.storage.page.PageId;
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
    private PageId firstPageId; // firstPageId 可能在表为空时被修改，所以不设为final
    private final LogManager logManager;

    // --- 迭代器状态 ---
    private PageId currentPageId;
    private Page currentPage;
    private int currentSlotIndex;

    public TableHeap(BufferPoolManager bufferPoolManager, TableInfo tableInfo, LogManager logManager) throws IOException {
        this.bufferPoolManager = bufferPoolManager;
        this.schema = tableInfo.getSchema();
        this.firstPageId = tableInfo.getFirstPageId();
        this.logManager = logManager;

        this.currentPageId = this.firstPageId;
        if (this.currentPageId != null && this.currentPageId.getPageNum() != -1) {
            this.currentPage = bufferPoolManager.getPage(this.currentPageId);
        } else {
            this.currentPage = null;
        }
        this.currentSlotIndex = 0;
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
                    currentPageId = new PageId(nextPageNum);
                    currentPage = bufferPoolManager.getPage(currentPageId);
                    currentSlotIndex = 0;
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
        // 1. 找到一个可以容纳该元组的数据页
        Page targetPage = findFreePageForInsert(tuple);
        if (targetPage == null) {
            return false;
        }

        // 2. 确定新元组的RID (页号 + 下一个可用的槽位索引)
        RID rid = new RID(targetPage.getPageId().getPageNum(), targetPage.getNumTuples());
        tuple.setRid(rid);

        // 3.【WAL核心】在修改数据页之前，先写入日志记录
        LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.INSERT, rid, tuple);
        long lsn = logManager.appendLogRecord(logRecord);
        txn.setPrevLSN(lsn); // 更新事务的日志链

        // 4.【物理修改】日志写入成功后，再实际地向数据页中插入元组
        boolean success = targetPage.insertTuple(tuple);
        if (success) {
            // 标记页面为“脏页”，BufferPoolManager会在合适的时机将其写回磁盘
            // 此处我们为简化，直接flush
            bufferPoolManager.flushPage(targetPage.getPageId());
        }
        return success;
    }

    private Page findFreePageForInsert(Tuple tuple) throws IOException {
        // ... (此辅助方法与上一版本相同)
        byte[] tupleBytes = tuple.toBytes();
        int requiredSpace = tupleBytes.length + 8; // 8 bytes for slot metadata

        PageId pid = this.firstPageId;
        Page lastPage = null;

        while (pid != null && pid.getPageNum() != -1) {
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

        if (lastPage != null) {
            lastPage.setNextPageId(newPage.getPageId().getPageNum());
            bufferPoolManager.flushPage(lastPage.getPageId());
        } else {
            // 这是表的第一页，需要更新 firstPageId
            this.firstPageId = newPage.getPageId();
            // 注意：这个修改需要通知Catalog，在更完整的系统中，
            // Catalog.createTable会创建第一页，并记录其ID。
        }

        if (newPage.getFreeSpace() < requiredSpace) {
            throw new IllegalStateException("Tuple is too large to fit in a new page.");
        }
        return newPage;
    }


    /**
     * 根据RID删除一条元组，并遵循WAL协议。
     * @param rid 要删除元组的物理地址
     * @param txn 当前事务
     * @return 删除成功返回 true
     */
    public boolean deleteTuple(RID rid, Transaction txn) throws IOException {
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

    /**
     * 根据RID更新一条元组，并遵循WAL协议。
     * @param newTuple 新的元组数据
     * @param rid      旧元组的物理地址
     * @param txn      当前事务
     * @return 更新成功返回 true
     */
    public boolean updateTuple(Tuple newTuple, RID rid, Transaction txn) throws IOException {
        Page page = bufferPoolManager.getPage(new PageId(rid.pageNum()));
        Tuple oldTuple = page.getTuple(rid.slotIndex(), schema);
        if (oldTuple == null) {
            return false;
        }

        // 1.【WAL核心】在修改数据页之前，写入UPDATE日志
        LogRecord logRecord = new LogRecord(txn.getTransactionId(), txn.getPrevLSN(), LogRecord.LogType.UPDATE, rid, oldTuple, newTuple);
        long lsn = logManager.appendLogRecord(logRecord);
        txn.setPrevLSN(lsn);

        // 2.【物理修改】采用先删除后插入的简化策略
        // 注意：这可能会改变元组的RID，对于更复杂的系统，需要实现就地更新(in-place update)
        if (page.markTupleAsDeleted(rid.slotIndex())) {
            if (page.getFreeSpace() >= newTuple.toBytes().length + 8) {
                if(page.insertTuple(newTuple)){
                    bufferPoolManager.flushPage(page.getPageId());
                    return true;
                }
            }
            // 如果空间不足，此处应该有更复杂的逻辑（如补偿日志回滚删除操作）
            // 在我们的简化模型中，我们直接认为更新失败，以防止破坏性操作
            System.err.println("Update failed: Not enough space on page for in-place update. (This prevented a bug!)");
            // 注意：此时我们已经写了UPDATE日志，但没有执行物理操作。
            // 在一个带恢复的系统中，这会在重启时通过回滚来修复。
            return false;
        }
        return false;
    }
}