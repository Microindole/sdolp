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
    private boolean done = false;
    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("deleted_rows", DataType.INT)));

    private final DeletePlanNode plan;
    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;

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
            // 先删除索引，再删除数据
            updateAllIndexesForDelete(tuple);
            if (tableHeap.deleteTuple(tuple.getRid(), txn)) {
                deletedCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(deletedCount)));
    }

    /**
     * 辅助方法：当删除一个元组前，更新该表上的所有索引。
     */
    private void updateAllIndexesForDelete(Tuple tuple) throws IOException {
        String tableName = plan.getTableInfo().getTableName();
        List<IndexInfo> indexes = catalog.getIndexesForTable(tableName);

        for (IndexInfo indexInfo : indexes) {
            BPlusTree index = new BPlusTree(bufferPoolManager, indexInfo.getRootPageId());
            int keyColumnIndex = plan.getTableInfo().getSchema().getColumnIndex(indexInfo.getColumnName());
            Value key = tuple.getValues().get(keyColumnIndex);
            index.delete(key);
            if (indexInfo.getRootPageId() != index.getRootPageId()) {
                System.out.println("[Executor] Index '" + indexInfo.getIndexName() + "' root page changed from " + indexInfo.getRootPageId() + " to " + index.getRootPageId() + ". Updating catalog...");
                catalog.updateIndexRootPageId(indexInfo.getIndexName(), index.getRootPageId());
            }
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