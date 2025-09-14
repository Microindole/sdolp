package org.csu.sdolp.executor.show;

import org.csu.sdolp.compiler.planner.plan.show.ShowCreateTablePlanNode;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;

public class ShowCreateTableExecutor implements TupleIterator {

    private final ShowCreateTablePlanNode plan;
    private boolean executed = false;

    public ShowCreateTableExecutor(ShowCreateTablePlanNode plan) {
        this.plan = plan;
    }

    @Override
    public Tuple next() throws IOException {
        // 我们不返回任何行，所以这个方法总是返回 null
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        // 因为我们不返回任何行，所以总是不存在下一行
        return false;
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}