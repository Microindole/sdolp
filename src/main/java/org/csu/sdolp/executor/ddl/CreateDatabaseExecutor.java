package org.csu.sdolp.executor.ddl;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.ddl.CreateDatabasePlanNode;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.Collections;

public class CreateDatabaseExecutor implements TupleIterator {

    private final CreateDatabasePlanNode plan;
    private final DatabaseManager dbManager;
    private boolean executed = false;

    public CreateDatabaseExecutor(CreateDatabasePlanNode plan, DatabaseManager dbManager) {
        this.plan = plan;
        this.dbManager = dbManager;
    }

    @Override
    public Tuple next() throws IOException {
        if (!executed) {
            dbManager.createDatabase(plan.getDbName());
            executed = true;
            return new Tuple(Collections.singletonList(new Value("Database '" + plan.getDbName() + "' created.")));
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return !executed;
    }

    @Override
    public Schema getOutputSchema() {
        return null;
    }
}