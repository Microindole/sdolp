package org.csu.sdolp.compiler.planner.plan.show;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

import java.util.List;

public class ShowDatabasesPlanNode extends PlanNode {
    private static final Schema SHOW_DATABASES_SCHEMA = new Schema(List.of(new Column("Database", DataType.VARCHAR)));
    public ShowDatabasesPlanNode() {
        super(SHOW_DATABASES_SCHEMA);
    }
}