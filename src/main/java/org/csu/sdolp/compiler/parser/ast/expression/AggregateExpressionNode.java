package org.csu.sdolp.compiler.parser.ast.expression;

import org.csu.sdolp.compiler.parser.ast.ExpressionNode;

/**
 * AST 节点: 表示一个聚合函数调用, e.g., COUNT(*), SUM(salary)
 * @param functionName 聚合函数名 (e.g., "COUNT", "SUM")
 * @param argument     函数参数 (可以是 '*' 或一个列名)
 * @param isStar       参数是否为 '*'
 */
public record AggregateExpressionNode(
        String functionName,
        ExpressionNode argument,
        boolean isStar
) implements ExpressionNode {
}
