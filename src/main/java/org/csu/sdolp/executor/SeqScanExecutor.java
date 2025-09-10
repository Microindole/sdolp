package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.Tuple;
import java.io.IOException;

public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;

    // *** 修改点：构造函数直接接收TableHeap ***
    public SeqScanExecutor(TableHeap tableHeap) {
        this.tableHeap = tableHeap;
    }

    @Override
    public Tuple next() throws IOException {
        return tableHeap.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        return tableHeap.hasNext();
    }
}