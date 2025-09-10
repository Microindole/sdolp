package org.csu.sdolp.executor.expressions;

import org.csu.sdolp.common.model.Tuple;
import java.io.IOException;

/**
 * 逻辑谓词，用于处理 AND 和 OR 运算。
 */
public class LogicalPredicate extends AbstractPredicate {
    private final AbstractPredicate left;
    private final AbstractPredicate right;
    private final String operator; // "AND" or "OR"

    public LogicalPredicate(AbstractPredicate left, AbstractPredicate right, String operator) {
        this.left = left;
        this.right = right;
        this.operator = operator;
    }

    @Override
    public boolean evaluate(Tuple tuple) throws IOException {
        return switch (operator) {
            case "AND" -> left.evaluate(tuple) && right.evaluate(tuple);
            case "OR" -> left.evaluate(tuple) || right.evaluate(tuple);
            default -> throw new UnsupportedOperationException("Unsupported logical operator: " + operator);
        };
    }
}
