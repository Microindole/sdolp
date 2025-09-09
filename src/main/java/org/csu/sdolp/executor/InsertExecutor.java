package org.csu.sdolp.executor;

import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.InsertPlanNode;

import java.io.IOException;
import java.util.Collections;

/**
 * 插入操作的执行器.
 */
public class InsertExecutor implements TupleIterator {

    private final InsertPlanNode plan;
    private final TableHeap tableHeap;
    private boolean done = false;

    public InsertExecutor(InsertPlanNode plan, TableHeap tableHeap) {
        this.plan = plan;
        this.tableHeap = tableHeap;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }

        int insertCount = 0;
        for (Tuple tuple : plan.getRawTuples()) {
            if (tableHeap.insertTuple(tuple)) {
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
}