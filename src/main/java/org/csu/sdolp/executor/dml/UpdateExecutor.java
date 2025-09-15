package org.csu.sdolp.executor.dml;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.parser.ast.expression.LiteralNode;
import org.csu.sdolp.compiler.parser.ast.expression.SetClauseNode;
import org.csu.sdolp.engine.ExpressionEvaluator;
import org.csu.sdolp.executor.TableHeap;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.index.BPlusTree;
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
                Value newValue = ExpressionEvaluator.evaluateToValue(clause.value(), schema, oldTuple);
                newValues.set(colIndex, newValue);
            }
            Tuple newTuple = new Tuple(newValues);

            String pkColumnName = schema.getPrimaryKeyColumnName();
            if (pkColumnName != null) {
                int pkIndex = schema.getColumnIndex(pkColumnName);
                Value oldPkValue = oldTuple.getValues().get(pkIndex);
                Value newPkValue = newTuple.getValues().get(pkIndex);
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
     * 辅助方法：更新所有索引。
     */
    private void updateAllIndexesForUpdate(Tuple oldTuple, Tuple newTuple, RID newRid) throws IOException {
        String tableName = tableHeap.getTableInfo().getTableName();
        List<IndexInfo> indexes = catalog.getIndexesForTable(tableName);

        for (IndexInfo indexInfo : indexes) {
            BPlusTree index = new BPlusTree(bufferPoolManager, indexInfo.getRootPageId());
            int keyColumnIndex = schema.getColumnIndex(indexInfo.getColumnName());

            Value oldKey = oldTuple.getValues().get(keyColumnIndex);
            Value newKey = newTuple.getValues().get(keyColumnIndex);

            index.delete(oldKey);
            index.insert(newKey, newRid);

            if (indexInfo.getRootPageId() != index.getRootPageId()) {
                System.out.println("[Executor] Index '" + indexInfo.getIndexName() + "' root page changed from " + indexInfo.getRootPageId() + " to " + index.getRootPageId() + ". Updating catalog...");
                catalog.updateIndexRootPageId(indexInfo.getIndexName(), index.getRootPageId());
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