package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;

import java.util.List;

public class ShowColumnsPlanNode extends PlanNode {

    private final String tableName;

    // 定义 SHOW COLUMNS 的输出 Schema
    private static final Schema SHOW_COLUMNS_SCHEMA = new Schema(List.of(
            new Column("Field", DataType.VARCHAR),
            new Column("Type", DataType.VARCHAR)
    ));

    public ShowColumnsPlanNode(String tableName) {
        super(SHOW_COLUMNS_SCHEMA);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}