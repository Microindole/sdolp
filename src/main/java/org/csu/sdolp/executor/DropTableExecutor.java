package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.DropTablePlanNode;

import java.io.IOException;
import java.util.Collections;

/**
 * 删除表的执行器
 */
public class DropTableExecutor implements TupleIterator {

    private final DropTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;

    public DropTableExecutor(DropTablePlanNode plan, Catalog catalog) {
        this.plan = plan;
        this.catalog = catalog;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        catalog.dropTable(plan.getTableName());
        done = true;
        return new Tuple(Collections.singletonList(new Value("Table '" + plan.getTableName() + "' dropped.")));
    }

    @Override
    public boolean hasNext() {
        return !done;
    }
}
