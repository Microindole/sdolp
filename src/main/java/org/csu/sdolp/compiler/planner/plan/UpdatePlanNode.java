package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.compiler.parser.ast.SetClauseNode;
import java.util.List;

public class UpdatePlanNode extends PlanNode {
    private final PlanNode child;
    private final TableInfo tableInfo;
    private final List<SetClauseNode> setClauses;

    public UpdatePlanNode(PlanNode child, TableInfo tableInfo, List<SetClauseNode> setClauses) {
        super(null); // UPDATE 不向上层返回元组
        this.child = child;
        this.tableInfo = tableInfo;
        this.setClauses = setClauses;
    }

    public PlanNode getChild() {
        return child;
    }

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public List<SetClauseNode> getSetClauses() {
        return setClauses;
    }
}