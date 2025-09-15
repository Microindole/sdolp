package org.csu.sdolp.executor.dml;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.dml.InsertPlanNode;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InsertExecutor implements TupleIterator {

    private final InsertPlanNode plan;
    private final TableHeap tableHeap;
    private final Transaction txn;
    private boolean done = false;
    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("inserted_rows", DataType.INT)));

    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;

    public InsertExecutor(InsertPlanNode plan, TableHeap tableHeap, Transaction txn, Catalog catalog, BufferPoolManager bufferPoolManager) {
        this.plan = plan;
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

        String primaryKeyColumnName = plan.getTableInfo().getSchema().getPrimaryKeyColumnName();
        int insertCount = 0;

        for (Tuple tuple : plan.getRawTuples()) {
            // 检查主键唯一性
            if (primaryKeyColumnName != null) {
                int pkIndex = plan.getTableInfo().getSchema().getColumnIndex(primaryKeyColumnName);
                Value pkValue = tuple.getValues().get(pkIndex);

                // 1. 获取主键索引信息
                IndexInfo pkIndexInfo = catalog.getIndex(plan.getTableInfo().getTableName(), primaryKeyColumnName);

                // 2. 如果主键索引存在，则执行查找
                if (pkIndexInfo != null) {
                    BPlusTree pkTree = new BPlusTree(bufferPoolManager, pkIndexInfo.getRootPageId());
                    if (pkTree.search(pkValue) != null) {
                        throw new RuntimeException("Primary key constraint violation: Duplicate key '" + pkValue + "'");
                    }
                }
            }
            if (tableHeap.insertTuple(tuple, txn)) {
                updateAllIndexesForInsert(tuple);
                insertCount++;
            }
        }

        done = true;
        return new Tuple(Collections.singletonList(new Value(insertCount)));
    }

    /**
     * 新增辅助方法：当插入一个元组后，更新该表上的所有索引。
     */
    private void updateAllIndexesForInsert(Tuple tuple) throws IOException {
        RID rid = tuple.getRid();
        String tableName = plan.getTableInfo().getTableName();
        List<IndexInfo> indexes = catalog.getIndexesForTable(tableName);

        for (IndexInfo indexInfo : indexes) {
            BPlusTree index = new BPlusTree(bufferPoolManager, indexInfo.getRootPageId());
            int keyColumnIndex = plan.getTableInfo().getSchema().getColumnIndex(indexInfo.getColumnName());
            Value key = tuple.getValues().get(keyColumnIndex);
            index.insert(key, rid);
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