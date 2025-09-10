package org.csu.sdolp.compiler.planner.plan;

/**
 * Limit 操作的执行计划节点
 */
public class LimitPlanNode extends PlanNode {
    private final PlanNode child;
    private final int limit;

    public LimitPlanNode(PlanNode child, int limit) {
        super(child.getOutputSchema()); // Limit 不改变 Schema
        this.child = child;
        this.limit = limit;
    }

    public PlanNode getChild() {
        return child;
    }

    public int getLimit() {
        return limit;
    }
}