package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;

import java.io.IOException;


public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;

    public SeqScanExecutor(BufferPoolManager bufferPoolManager, TableInfo tableInfo) throws IOException {
        this.tableHeap = new TableHeap(bufferPoolManager, tableInfo);
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