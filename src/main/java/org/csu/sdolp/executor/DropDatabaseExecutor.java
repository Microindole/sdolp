package org.csu.sdolp.executor;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.DropDatabasePlanNode;

import java.io.IOException;
import java.util.Collections;

public class DropDatabaseExecutor implements TupleIterator {

    private final DropDatabasePlanNode plan;
    private final DatabaseManager dbManager;
    private boolean executed = false;

    public DropDatabaseExecutor(DropDatabasePlanNode plan, DatabaseManager dbManager) {
        this.plan = plan;
        this.dbManager = dbManager;
    }

    @Override
    public Tuple next() throws IOException {
        if (!executed) {
            dbManager.dropDatabase(plan.getDbName());
            executed = true;
            return new Tuple(Collections.singletonList(new Value("Database '" + plan.getDbName() + "' dropped.")));
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