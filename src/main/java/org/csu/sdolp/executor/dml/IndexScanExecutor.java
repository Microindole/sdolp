package org.csu.sdolp.executor.dml;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.planner.plan.query.IndexScanPlanNode;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class IndexScanExecutor implements TupleIterator {

    private final IndexScanPlanNode plan;
    private final TableHeap tableHeap;
    private final BufferPoolManager bufferPoolManager;
    private final Transaction txn;
    private Iterator<Tuple> resultIterator;

    public IndexScanExecutor(IndexScanPlanNode plan, TableHeap tableHeap, BufferPoolManager bufferPoolManager, Transaction txn) {
        this.plan = plan;
        this.tableHeap = tableHeap;
        this.bufferPoolManager = bufferPoolManager;
        this.txn = txn;
    }

    private void performScan() throws IOException {
        List<Tuple> results = new LinkedList<>();
        // 1. 初始化 B+ 树
        BPlusTree index = new BPlusTree(bufferPoolManager, plan.getIndexInfo().getRootPageId());

        // 2. 使用 B+ 树的 search 方法查找 RID
        RID rid = index.search(plan.getSearchKey());

        // 3. 如果找到了 RID，就从 TableHeap 中获取完整的元组
        if (rid != null) {
            Tuple tuple = tableHeap.getTuple(rid, txn);
            if (tuple != null) {
                results.add(tuple);
            }
        }
        // 当前实现只支持单点查找。范围查找需要修改B+树以支持迭代器。
        this.resultIterator = results.iterator();
    }


    @Override
    public boolean hasNext() throws IOException {
        if (resultIterator == null) {
            performScan();
        }
        return resultIterator.hasNext();
    }

    @Override
    public Tuple next() throws IOException {
        if (resultIterator == null) {
            performScan();
        }
        return resultIterator.next();
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}