package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.ShowTablesPlanNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Executes the "SHOW TABLES" plan by fetching table names from the catalog.
 */
public class ShowTablesExecutor implements TupleIterator {

    private final ShowTablesPlanNode plan;
    private final Catalog catalog;
    private Iterator<Tuple> tableTupleIterator;
    private boolean executed = false;

    public ShowTablesExecutor(ShowTablesPlanNode plan, Catalog catalog) {
        this.plan = plan;
        this.catalog = catalog;
    }

    private void execute() {
        if (executed) {
            return;
        }
        // Get all user-defined table names from the catalog.
        List<String> tableNames = catalog.getTableNames();
        // Convert each table name into a single-value Tuple.
        List<Tuple> resultTuples = tableNames.stream()
                .map(tableName -> new Tuple(List.of(new Value(tableName))))
                .collect(Collectors.toList());
        this.tableTupleIterator = resultTuples.iterator();
        this.executed = true;
    }

    @Override
    public Tuple next() throws IOException {
        if (!executed) {
            execute();
        }
        if (tableTupleIterator.hasNext()) {
            return tableTupleIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (!executed) {
            execute();
        }
        return tableTupleIterator.hasNext();
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}
