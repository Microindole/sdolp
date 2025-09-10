package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.*;
import org.csu.sdolp.compiler.planner.plan.AlterTablePlanNode;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.transaction.log.LogRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class AlterTableExecutor implements TupleIterator {

    private final AlterTablePlanNode plan;
    private final Catalog catalog;
    private boolean done = false;
    private static final Schema RESULT_SCHEMA = new Schema(List.of(new Column("message", DataType.VARCHAR)));
    private final Transaction txn;
    private final LogManager logManager;

    public AlterTableExecutor(AlterTablePlanNode plan, Catalog catalog, Transaction txn, LogManager logManager) {
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

        LogRecord logRecord = new LogRecord(
                txn.getTransactionId(),
                txn.getPrevLSN(),
                LogRecord.LogType.ALTER_TABLE,
                plan.getTableName(),
                plan.getNewColumn()
        );
        long lsn = logManager.appendLogRecord(logRecord);
        txn.setPrevLSN(lsn);

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