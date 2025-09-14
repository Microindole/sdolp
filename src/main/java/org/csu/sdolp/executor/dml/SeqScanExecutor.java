package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;

public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;
    //private boolean isInitialized = false;

    // *** 修改点：构造函数直接接收TableHeap ***
    public SeqScanExecutor(TableHeap tableHeap, Transaction txn) throws IOException {
        this.tableHeap = tableHeap;
        // *** 修改点：初始化 TableHeap 的迭代器 ***
        this.tableHeap.initIterator(txn);
        //this.isInitialized = true;
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