package org.csu.sdolp.executor.dml;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.dml.DeletePlanNode;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteExecutor implements TupleIterator {
    private final TupleIterator child;
    private final TableHeap tableHeap;
    private final Transaction txn;
    private boolean done = false; // 修复点1：使用 'done'
    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("deleted_rows", DataType.INT)));

    // 修复点2：添加必要的成员变量
    private final DeletePlanNode plan;
    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;

    // 修复点3：修改构造函数以接收所有依赖
    public DeleteExecutor(DeletePlanNode plan, TupleIterator child, TableHeap tableHeap, Transaction txn, Catalog catalog, BufferPoolManager bufferPoolManager) {
        this.plan = plan;
        this.child = child;
        this.tableHeap = tableHeap;
        this.txn = txn;
        this.catalog = catalog;
        this.bufferPoolManager = bufferPoolManager;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }

        List<Tuple> tuplesToDelete = new ArrayList<>();
        while (child.hasNext()) {
            tuplesToDelete.add(child.next());
        }

        int deletedCount = 0;
        for (Tuple tuple : tuplesToDelete) {
            // 核心逻辑：先删除索引，再删除数据
            updateAllIndexesForDelete(tuple);
            if (tableHeap.deleteTuple(tuple.getRid(), txn)) {
                deletedCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(deletedCount)));
    }

    /**
     * 新增辅助方法：当删除一个元组前，更新该表上的所有索引。
     */
    private void updateAllIndexesForDelete(Tuple tuple) throws IOException {
        String tableName = plan.getTableInfo().getTableName();
        List<IndexInfo> indexes = catalog.getIndexesForTable(tableName);

        for (IndexInfo indexInfo : indexes) {
            BPlusTree index = new BPlusTree(bufferPoolManager, indexInfo.getRootPageId());
            int keyColumnIndex = plan.getTableInfo().getSchema().getColumnIndex(indexInfo.getColumnName());
            Value key = tuple.getValues().get(keyColumnIndex);
            index.delete(key);
        }
    }

    @Override
    public boolean hasNext() {
        return !done;
    }

    @Override
    public Schema getOutputSchema() {
        return AFFECTED_ROWS_SCHEMA;
    }
}