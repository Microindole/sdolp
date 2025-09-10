package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteExecutor implements TupleIterator {
    private final TupleIterator child;
    private final TableHeap tableHeap;
    private final Transaction txn; // *** 新增成员变量 ***
    private boolean done = false;

    // *** 修改点：构造函数增加Transaction参数 ***
    public DeleteExecutor(TupleIterator child, TableHeap tableHeap, Transaction txn) {
        this.child = child;
        this.tableHeap = tableHeap;
        this.txn = txn;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        int deletedCount = 0;
        while (child.hasNext()) {
            Tuple tupleToDelete = child.next();
            // *** 修改点：调用deleteTuple时传入Transaction ***
            if (tableHeap.deleteTuple(tupleToDelete.getRid(), txn)) {
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