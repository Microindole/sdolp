package org.csu.sdolp.compiler.planner.plan.ddl;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

public class CreateIndexPlanNode extends PlanNode {
    private final String indexName;
    private final String tableName;
    private final String columnName;
    private final TableInfo tableInfo;

    public CreateIndexPlanNode(String indexName, String tableName, String columnName, TableInfo tableInfo) {
        super(null); // DDL操作通常没有输出模式
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.tableInfo = tableInfo;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }
}