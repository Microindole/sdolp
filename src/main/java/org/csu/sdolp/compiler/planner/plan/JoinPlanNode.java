package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;

import java.util.ArrayList;
import java.util.List;

public class JoinPlanNode extends PlanNode {
    private final PlanNode left;
    private final PlanNode right;
    private final ExpressionNode joinCondition; // 连接条件

    public JoinPlanNode(PlanNode left, PlanNode right, ExpressionNode joinCondition) {
        super(createJoinSchema(left.getOutputSchema(), right.getOutputSchema()));
        this.left = left;
        this.right = right;
        this.joinCondition = joinCondition;
    }

    public PlanNode getLeft() { return left; }
    public PlanNode getRight() { return right; }
    public ExpressionNode getJoinCondition() { return joinCondition; }

    // 辅助方法，用于合并左右两个子节点的 Schema
    private static Schema createJoinSchema(Schema leftSchema, Schema rightSchema) {
        List<Column> allColumns = new ArrayList<>(leftSchema.getColumns());
        allColumns.addAll(rightSchema.getColumns());
        return new Schema(allColumns);
    }
}