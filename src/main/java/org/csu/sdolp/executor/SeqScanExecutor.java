package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.storage.buffer.BufferPoolManager;

import java.io.IOException;

/**
 * 顺序扫描执行器。
 * 这是执行计划中的一个节点，它通过使用 TableHeap 来实现对表的顺序扫描。
 */
public class SeqScanExecutor implements TupleIterator {

    private final TableHeap tableHeap;

    public SeqScanExecutor(BufferPoolManager bufferPoolManager, TableInfo tableInfo) throws IOException {
        // SeqScanExecutor 的核心就是初始化一个 TableHeap
        this.tableHeap = new TableHeap(bufferPoolManager, tableInfo);
    }

    @Override
    public Tuple next() throws IOException {
        // 将工作完全委托给 tableHeap
        return tableHeap.next();
    }

    @Override
    public boolean hasNext() throws IOException {
        // 将工作完全委托给 tableHeap
        return tableHeap.hasNext();
    }
}