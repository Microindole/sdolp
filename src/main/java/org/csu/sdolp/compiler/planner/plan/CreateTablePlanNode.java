package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Schema;

/**
 * @author hidyouth
 * @description: 创建表的执行计划节点
 */
public class CreateTablePlanNode extends PlanNode {
    private final String tableName;

    public CreateTablePlanNode(String tableName, Schema schema) {
        super(schema); // CreateTable "输出"的是新表的Schema
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
