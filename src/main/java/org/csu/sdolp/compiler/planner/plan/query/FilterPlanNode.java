package org.csu.sdolp.compiler.planner.plan.query;

import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

/**
 * @author hidyouth
 * @description: 过滤操作的执行计划节点
 */
public class FilterPlanNode extends PlanNode {
    private final PlanNode child;
    private final ExpressionNode predicate;

    public FilterPlanNode(PlanNode child, ExpressionNode predicate) {
        super(child.getOutputSchema()); // Filter 不改变 Schema
        this.child = child;
        this.predicate = predicate;
    }

    public PlanNode getChild() {
        return child;
    }

    public ExpressionNode getPredicate() {
        return predicate;
    }
}
