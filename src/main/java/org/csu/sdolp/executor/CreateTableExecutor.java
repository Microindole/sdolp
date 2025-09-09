package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.CreateTablePlanNode;

import java.io.IOException;
import java.util.Collections;

/**
 * 创建表的执行器.
 */
public class CreateTableExecutor implements TupleIterator {

    private final CreateTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;

    public CreateTableExecutor(CreateTablePlanNode plan, Catalog catalog) {
        this.plan = plan;
        this.catalog = catalog;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        catalog.createTable(plan.getTableName(), plan.getOutputSchema());
        done = true;
        // DDL 语句通常返回一个表示成功的元组，或不返回
        return new Tuple(Collections.singletonList(new Value("Table '" + plan.getTableName() + "' created.")));
    }

    @Override
    public boolean hasNext() throws IOException {
        return !done;
    }
}