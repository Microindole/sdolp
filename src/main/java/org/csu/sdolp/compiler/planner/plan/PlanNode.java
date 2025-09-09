package org.csu.sdolp.compiler.planner.plan;

import org.csu.sdolp.common.model.Schema;

/**
 * @author hidyouth
 * @description: 所有执行计划节点的抽象基类
 */
public abstract class PlanNode {
    // 每个计划节点都应该能描述它输出的元组的模式(Schema)
    private final Schema outputSchema;

    public PlanNode(Schema outputSchema) {
        this.outputSchema = outputSchema;
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }
}
