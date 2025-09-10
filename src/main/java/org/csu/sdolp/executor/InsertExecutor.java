package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.InsertPlanNode;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class InsertExecutor implements TupleIterator {

    private final InsertPlanNode plan;
    private final TableHeap tableHeap;
    private final Transaction txn; // *** 新增成员变量 ***
    private boolean done = false;
    private static final Schema AFFECTED_ROWS_SCHEMA = new Schema(List.of(new Column("inserted_rows", DataType.INT)));

    // *** 修改点：构造函数增加Transaction参数 ***
    public InsertExecutor(InsertPlanNode plan, TableHeap tableHeap, Transaction txn) {
        this.plan = plan;
        this.tableHeap = tableHeap;
        this.txn = txn;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }

        int insertCount = 0;
        for (Tuple tuple : plan.getRawTuples()) {
            // *** 修改点：调用insertTuple时传入Transaction ***
            if (tableHeap.insertTuple(tuple, txn)) {
                insertCount++;
            }
        }
        done = true;
        return new Tuple(Collections.singletonList(new Value(insertCount)));
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