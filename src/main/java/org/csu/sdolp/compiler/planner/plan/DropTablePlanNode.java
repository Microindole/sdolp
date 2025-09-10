package org.csu.sdolp.compiler.planner.plan;

/**
 * 删除表的执行计划节点
 */
public class DropTablePlanNode extends PlanNode {
    private final String tableName;

    public DropTablePlanNode(String tableName) {
        super(null); // DDL 不向上层返回元组
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
