package org.csu.sdolp.executor;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.planner.plan.ShowTablesPlanNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * 执行 SHOW TABLES 语句的执行器。
 */
public class ShowTablesExecutor implements TupleIterator {

    private final ShowTablesPlanNode plan;
    private final Catalog catalog;
    private final Schema outputSchema;
    private Iterator<Tuple> resultIterator;

    public ShowTablesExecutor(ShowTablesPlanNode plan, Catalog catalog) {
        this.plan = plan;
        this.catalog = catalog;
        // SHOW TABLES 的输出 Schema 是固定的
        this.outputSchema = new Schema(List.of(new Column("Tables", DataType.VARCHAR)));
        this.resultIterator = null;
    }

    private void generateTableList() {
        // 从 Catalog 获取所有表名
        List<String> tableNames = catalog.getAllTableNames();

        // 过滤掉内部系统表 (以 "_" 开头)
        tableNames.removeIf(name -> name.startsWith("_"));

        // 按字母排序
        Collections.sort(tableNames);

        List<Tuple> resultTuples = new ArrayList<>();
        for (String tableName : tableNames) {
            resultTuples.add(new Tuple(Collections.singletonList(new Value(tableName))));
        }
        this.resultIterator = resultTuples.iterator();
    }


    @Override
    public Tuple next() throws IOException {
        if (resultIterator == null) {
            generateTableList();
        }
        if (resultIterator.hasNext()) {
            return resultIterator.next();
        }
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (resultIterator == null) {
            generateTableList();
        }
        return resultIterator.hasNext();
    }

    @Override
    public Schema getOutputSchema() {
        return outputSchema;
    }
}