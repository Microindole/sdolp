package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteExecutor implements TupleIterator {
    private final TupleIterator child;
    private final TableHeap tableHeap;
    private final Transaction txn;
    private boolean done = false;
    private final LockManager lockManager;
    private final PageId firstPageId;

    // *** 修改点：构造函数增加Transaction参数 ***
    public DeleteExecutor(TupleIterator child, TableHeap tableHeap, Transaction txn) {
        this.child = child;
        this.tableHeap = tableHeap;
        this.txn = txn;
        this.lockManager = tableHeap.getLockManager();
        this.firstPageId = tableHeap.getFirstPageId();
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        // 在扫描前获取排他锁
        try {
            if (firstPageId != null && firstPageId.getPageNum() != -1) {
                System.out.println("[DEBUG] DeleteExecutor acquiring X-lock on table (page " + firstPageId.getPageNum() + ") before scanning.");
                lockManager.lockExclusive(txn, firstPageId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring table lock for DELETE.", e);
        }

        // *** 核心修复：采用两阶段删除 ***
        // 阶段一：收集所有待删除元组的RID
        List<RID> ridsToDelete = new ArrayList<>();
        while (child.hasNext()) {
            ridsToDelete.add(child.next().getRid());
        }

        // 阶段二：执行删除
        int deletedCount = 0;
        for (RID rid : ridsToDelete) {
            if (tableHeap.deleteTuple(rid, txn,false)) {
                deletedCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(deletedCount)));
    }

    @Override
    public boolean hasNext() throws IOException {
        return !done;
    }
}