package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;

import java.util.List;

public class ShowCreateTablePlanNode extends PlanNode {

    private final String tableName;

    // 定义 SHOW CREATE TABLE 的输出 Schema
    private static final Schema SHOW_CREATE_TABLE_SCHEMA = new Schema(List.of(
            new Column("Table", DataType.VARCHAR),
            new Column("Create Table", DataType.VARCHAR)
    ));

    public ShowCreateTablePlanNode(String tableName) {
        super(SHOW_CREATE_TABLE_SCHEMA);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}