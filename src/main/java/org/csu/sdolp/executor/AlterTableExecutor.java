package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.AlterTablePlanNode;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 修改表的执行器
 */
public class AlterTableExecutor implements TupleIterator {

    private final AlterTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;
    private static final Schema RESULT_SCHEMA = new Schema(List.of(new Column("message", DataType.VARCHAR)));

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

    @Override
    public Schema getOutputSchema() {
        return RESULT_SCHEMA;
    }
}
