package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;

public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;

    // --- 核心修改点 ---
    // 新增一个可以指定锁模式的构造函数
    public SeqScanExecutor(TableHeap tableHeap, Transaction txn, LockManager.LockMode lockMode) throws IOException {
        this.tableHeap = tableHeap;
        this.tableHeap.initIterator(txn, lockMode);
    }

    // 保留原有的构造函数，默认为共享锁模式，用于 SELECT 语句
    public SeqScanExecutor(TableHeap tableHeap, Transaction txn) throws IOException {
        this(tableHeap, txn, LockManager.LockMode.SHARED);
    }
    @Override
    public Tuple next() throws IOException {
        return tableHeap.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        return tableHeap.hasNext();
    }
    @Override
    public Schema getOutputSchema() {
        return tableHeap.getOutputSchema();
    }


}