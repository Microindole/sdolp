package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;

import java.util.List;

/**
 * "SHOW TABLES" statement execution plan node.
 */
public class ShowTablesPlanNode extends PlanNode {

    private static final Schema SHOW_TABLES_SCHEMA = new Schema(List.of(new Column("TABLES", DataType.VARCHAR)));

    public ShowTablesPlanNode() {
        super(SHOW_TABLES_SCHEMA);
    }
}
