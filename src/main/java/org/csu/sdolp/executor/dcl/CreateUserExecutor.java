package org.csu.sdolp.executor.dcl;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.dcl.CreateUserPlanNode;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.transaction.Transaction;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 执行 CREATE USER 语句的执行器。
 */
public class CreateUserExecutor implements TupleIterator {

    private final CreateUserPlanNode plan;
    private final Catalog catalog;
    private final Transaction txn; // 尽管 DCL 通常不被认为在事务中，但我们的框架需要它
    private boolean done = false;

    private static final Schema RESULT_SCHEMA = new Schema(List.of(new Column("message", DataType.VARCHAR)));

    public CreateUserExecutor(CreateUserPlanNode plan, Catalog catalog, Transaction txn) {
        this.plan = plan;
        this.catalog = catalog;
        this.txn = txn;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }

        // 调用 Catalog 来执行核心业务逻辑
        catalog.createUser(plan.getUsername(), plan.getPassword());

        done = true;
        // DCL/DDL 语句通常返回一个表示成功的消息元组
        return new Tuple(Collections.singletonList(new Value("User '" + plan.getUsername() + "' created.")));
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