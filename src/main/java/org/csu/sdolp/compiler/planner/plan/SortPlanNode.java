package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.compiler.parser.ast.OrderByClauseNode;

/**
 * 排序操作的执行计划节点
 */
public class SortPlanNode extends PlanNode {
    private final PlanNode child;
    private final OrderByClauseNode orderBy;

    public SortPlanNode(PlanNode child, OrderByClauseNode orderBy) {
        super(child.getOutputSchema()); // 排序不改变 Schema
        this.child = child;
        this.orderBy = orderBy;
    }

    public PlanNode getChild() {
        return child;
    }

    public OrderByClauseNode getOrderBy() {
        return orderBy;
    }
}