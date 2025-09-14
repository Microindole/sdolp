package org.csu.sdolp.compiler.planner.plan.query;

import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.parser.ast.expression.AggregateExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.planner.plan.PlanNode;

import java.util.List;

/**
 * 聚合操作的执行计划节点
 */
public class AggregatePlanNode extends PlanNode {
    private final PlanNode child;
    private final List<IdentifierNode> groupBys;
    private final List<AggregateExpressionNode> aggregates;

    public AggregatePlanNode(PlanNode child, List<IdentifierNode> groupBys,
                             List<AggregateExpressionNode> aggregates, Schema outputSchema) {
        super(outputSchema);
        this.child = child;
        this.groupBys = groupBys;
        this.aggregates = aggregates;
    }

    public PlanNode getChild() {
        return child;
    }

    public List<IdentifierNode> getGroupBys() {
        return groupBys;
    }

    public List<AggregateExpressionNode> getAggregates() {
        return aggregates;
    }
}
