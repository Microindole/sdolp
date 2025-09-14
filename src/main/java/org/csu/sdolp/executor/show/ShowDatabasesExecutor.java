package org.csu.sdolp.executor.show;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.show.ShowDatabasesPlanNode;
import org.csu.sdolp.executor.TupleIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowDatabasesExecutor implements TupleIterator {

    private final ShowDatabasesPlanNode plan;
    private final DatabaseManager dbManager;
    private Iterator<Tuple> resultIterator;

    public ShowDatabasesExecutor(ShowDatabasesPlanNode plan, DatabaseManager dbManager) {
        this.plan = plan;
        this.dbManager = dbManager;
    }

    private void generateDatabaseList() {
        List<String> dbNames = dbManager.listDatabases();
        List<Tuple> resultTuples = dbNames.stream()
                .map(name -> new Tuple(List.of(new Value(name))))
                .collect(Collectors.toList());
        this.resultIterator = resultTuples.iterator();
    }

    @Override
    public Tuple next() throws IOException {
        if (resultIterator == null) {
            generateDatabaseList();
        }
        if (resultIterator.hasNext()) {
            return resultIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (resultIterator == null) {
            generateDatabaseList();
        }
        return resultIterator.hasNext();
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}