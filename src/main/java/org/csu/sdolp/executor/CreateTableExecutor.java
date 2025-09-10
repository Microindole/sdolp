package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.CreateTablePlanNode;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * 创建表的执行器.
 */
public class CreateTableExecutor implements TupleIterator {

    private final CreateTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;
    private static final Schema RESULT_SCHEMA = new Schema(List.of(new Column("message", DataType.VARCHAR)));
    private final Transaction txn;
    private final LogManager logManager;

    public CreateTableExecutor(CreateTablePlanNode plan, Catalog catalog, Transaction txn, LogManager logManager) {
        this.plan = plan;
        this.catalog = catalog;
        this.txn = txn;
        this.logManager = logManager;
    }

    @Override
    public Tuple next() throws IOException {
        if (done) {
            return null;
        }
        // 在物理操作前，先写日志
        LogRecord logRecord = new LogRecord(
                txn.getTransactionId(),
                txn.getPrevLSN(),
                LogRecord.LogType.CREATE_TABLE,
                plan.getTableName(),
                plan.getOutputSchema()
        );
        long lsn = logManager.appendLogRecord(logRecord);
        txn.setPrevLSN(lsn);

        // 日志写入成功后，再执行物理操作
        catalog.createTable(plan.getTableName(), plan.getOutputSchema());
        done = true;
        // DDL 语句通常返回一个表示成功的元组，或不返回
        return new Tuple(Collections.singletonList(new Value("Table '" + plan.getTableName() + "' created.")));
    }

    @Override
    public boolean hasNext() throws IOException {
        return !done;
    }

    @Override
    public Schema getOutputSchema() {
        return RESULT_SCHEMA;
    }
}