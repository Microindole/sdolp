package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;

/**
 * 修改表的执行计划节点
 */
public class AlterTablePlanNode extends PlanNode {
    private final String tableName;
    private final Column newColumn;

    public AlterTablePlanNode(String tableName, Column newColumn) {
        super(null); // DDL 不向上层返回元组
        this.tableName = tableName;
        this.newColumn = newColumn;
    }

    public String getTableName() {
        return tableName;
    }

    public Column getNewColumn() {
        return newColumn;
    }
}
