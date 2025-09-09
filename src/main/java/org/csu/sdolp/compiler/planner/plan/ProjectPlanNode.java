package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Schema;

/**
 * @author hidyouth
 * @description: 投影操作的执行计划节点
 */
public class ProjectPlanNode extends PlanNode {
    private final PlanNode child;

    public ProjectPlanNode(PlanNode child, Schema outputSchema) {
        super(outputSchema); // Project 会改变 Schema
        this.child = child;
    }

    public PlanNode getChild() {
        return child;
    }
}
