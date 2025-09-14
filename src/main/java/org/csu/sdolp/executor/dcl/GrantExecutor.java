package org.csu.sdolp.executor.dcl;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.dcl.GrantPlanNode;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 执行 GRANT 语句的执行器。
 */
public class GrantExecutor implements TupleIterator {

    private final GrantPlanNode plan;
    private final Catalog catalog;
    private final Transaction txn;
    private boolean done = false;

    private static final Schema RESULT_SCHEMA = new Schema(List.of(new Column("message", DataType.VARCHAR)));

    public GrantExecutor(GrantPlanNode plan, Catalog catalog, Transaction txn) {
        this.plan = plan;
        this.catalog = catalog;
        this.txn = txn;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        
        // 循环为每个权限类型调用 Catalog
        for (String privilege : plan.getPrivileges()) {
            catalog.grantPrivilege(plan.getUsername(), plan.getTableName(), privilege);
        }

        done = true;
        return new Tuple(Collections.singletonList(new Value("Grants successful for user '" + plan.getUsername() + "'.")));
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