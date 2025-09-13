package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;

import java.util.List;

public class ShowDatabasesPlanNode extends PlanNode {
    private static final Schema SHOW_DATABASES_SCHEMA = new Schema(List.of(new Column("Database", DataType.VARCHAR)));
    public ShowDatabasesPlanNode() {
        super(SHOW_DATABASES_SCHEMA);
    }
}