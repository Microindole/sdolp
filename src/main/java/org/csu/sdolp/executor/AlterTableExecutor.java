package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.AlterTablePlanNode;

import java.io.IOException;
import java.util.Collections;

/**
 * 修改表的执行器
 */
public class AlterTableExecutor implements TupleIterator {

    private final AlterTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;

    public AlterTableExecutor(AlterTablePlanNode plan, Catalog catalog) {
        this.plan = plan;
        this.catalog = catalog;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        catalog.addColumn(plan.getTableName(), plan.getNewColumn());
        done = true;
        return new Tuple(Collections.singletonList(new Value("Table '" + plan.getTableName() + "' altered.")));
    }

    @Override
    public boolean hasNext() {
        return !done;
    }
}
