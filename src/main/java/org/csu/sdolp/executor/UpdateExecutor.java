package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.parser.ast.LiteralNode;
import org.csu.sdolp.compiler.parser.ast.SetClauseNode;
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
    private boolean done = false;

    private final LockManager lockManager;
    private final PageId firstPageId;
    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("updated_rows", DataType.INT)));

    // *** 修改点：构造函数增加Transaction参数 ***
    public UpdateExecutor(TupleIterator child, TableHeap tableHeap, Schema schema, List<SetClauseNode> setClauses, Transaction txn) {
        this.child = child;
        this.tableHeap = tableHeap;
        this.schema = schema;
        this.setClauses = setClauses;
        this.txn = txn;
        this.lockManager = tableHeap.getLockManager();
        this.firstPageId = tableHeap.getFirstPageId();
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        try {
            if (firstPageId != null && firstPageId.getPageNum() != -1) {
                System.out.println("[DEBUG] UpdateExecutor acquiring X-lock on table (page " + firstPageId.getPageNum() + ") before scanning.");
                lockManager.lockExclusive(txn, firstPageId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Thread interrupted while acquiring table lock for UPDATE.", e);
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

            // *** 修改点：调用updateTuple时传入Transaction ***
            if (tableHeap.updateTuple(newTuple, oldTuple.getRid(), txn, false)) {
                updatedCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(updatedCount)));
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