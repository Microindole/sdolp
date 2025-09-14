package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.parser.ast.LiteralNode;
import org.csu.sdolp.compiler.parser.ast.SetClauseNode;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.index.BPlusTree;
import org.csu.sdolp.storage.page.PageId;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UpdateExecutor implements TupleIterator {
    private final TupleIterator child;
    private final TableHeap tableHeap;
    private final Schema schema;
    private final List<SetClauseNode> setClauses;
    private final Transaction txn;
    private final Catalog catalog;
    private final BufferPoolManager bufferPoolManager;
    private boolean done = false;

    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("updated_rows", DataType.INT)));

    public UpdateExecutor(TupleIterator child, TableHeap tableHeap, Schema schema, List<SetClauseNode> setClauses, Transaction txn, Catalog catalog, BufferPoolManager bufferPoolManager) {
        this.child = child;
        this.tableHeap = tableHeap;
        this.schema = schema;
        this.setClauses = setClauses;
        this.txn = txn;
        this.catalog = catalog;
        this.bufferPoolManager = bufferPoolManager;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }

        List<Tuple> tuplesToUpdate = new ArrayList<>();
        while (child.hasNext()) {
            tuplesToUpdate.add(child.next());
        }
        int updatedCount = 0;
        for (Tuple oldTuple : tuplesToUpdate) {
            List<Value> newValues = new ArrayList<>(oldTuple.getValues());
            for (SetClauseNode clause : setClauses) {
                String colName = clause.column().getName();
                int colIndex = getColumnIndex(schema, colName);
                Value newValue = getLiteralValue((LiteralNode) clause.value());
                newValues.set(colIndex, newValue);
            }
            Tuple newTuple = new Tuple(newValues);

            // 2. 检查主键约束（如果被更新）
            String pkColumnName = schema.getPrimaryKeyColumnName();
            if (pkColumnName != null) {
                int pkIndex = schema.getColumnIndex(pkColumnName);
                Value oldPkValue = oldTuple.getValues().get(pkIndex);
                Value newPkValue = newTuple.getValues().get(pkIndex);

                // 如果主键值被修改，并且新值已存在，则抛出异常
                if (!oldPkValue.equals(newPkValue)) {
                    IndexInfo pkIndexInfo = catalog.getIndex(tableHeap.getTableInfo().getTableName(), pkColumnName);
                    if (pkIndexInfo != null) {
                        BPlusTree pkTree = new BPlusTree(bufferPoolManager, pkIndexInfo.getRootPageId());
                        if (pkTree.search(newPkValue) != null) {
                            throw new RuntimeException("Primary key constraint violation: Cannot update to existing key '" + newPkValue + "'");
                        }
                    }
                }
            }

            // 3. 执行物理更新，并返回新元组的RID
            // 修复点：修改 updateTuple 的返回值以匹配 TableHeap 的新接口
            RID newRid = tableHeap.updateTuple(newTuple, oldTuple.getRid(), txn);
            if (newRid != null) {
                updateAllIndexesForUpdate(oldTuple, newTuple, newRid);
                updatedCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(updatedCount)));
    }

    /**
     * 新增辅助方法：更新所有索引。
     */
    private void updateAllIndexesForUpdate(Tuple oldTuple, Tuple newTuple, RID newRid) throws IOException {
        String tableName = tableHeap.getTableInfo().getTableName();
        List<IndexInfo> indexes = catalog.getIndexesForTable(tableName);

        for (IndexInfo indexInfo : indexes) {
            BPlusTree index = new BPlusTree(bufferPoolManager, indexInfo.getRootPageId());
            int keyColumnIndex = schema.getColumnIndex(indexInfo.getColumnName());

            Value oldKey = oldTuple.getValues().get(keyColumnIndex);
            Value newKey = newTuple.getValues().get(keyColumnIndex);

            // 如果索引键值发生变化，则更新索引
            if (!oldKey.equals(newKey)) {
                index.delete(oldKey);
                index.insert(newKey, newRid);
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return !done;
    }

    // --- 辅助方法 (可以从 QueryProcessor 移到这里或一个公共类) ---
    private int getColumnIndex(Schema schema, String columnName) {
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalStateException("Column '" + columnName + "' not found in schema.");
    }

    private Value getLiteralValue(LiteralNode literalNode) {
        String lexeme = literalNode.literal().lexeme();
        return switch (literalNode.literal().type()) {
            case INTEGER_CONST -> new Value(Integer.parseInt(lexeme));
            case STRING_CONST -> new Value(lexeme);
            default -> throw new IllegalStateException("Unsupported literal type.");
        };
    }

    @Override
    public Schema getOutputSchema() {
        return AFFECTED_ROWS_SCHEMA;
    }
}