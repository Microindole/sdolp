package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.catalog.TableInfo;

public class DeletePlanNode extends PlanNode {
    private final PlanNode child;
    private final TableInfo tableInfo;

    public DeletePlanNode(PlanNode child, TableInfo tableInfo) {
        super(null); // DELETE 不向上层返回元组
        this.child = child;
        this.tableInfo = tableInfo;
    }

    public PlanNode getChild() {
        return child;
    }
    
    public TableInfo getTableInfo(){
        return tableInfo;
    }
}