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
    // 重写 toString() 方法以生成正确的 SQL 风格的列名
    // 这是让 HAVING 子句能够正确找到聚合结果列的关键
    @Override
    public String toString() {
        if (isStar) {
            return functionName + "(*)";
        } else {
            // 我们需要递归调用参数的 toString() 来正确处理 qualified name, e.g., table.column
            return functionName + "(" + argument.toString() + ")";
        }
    }
}
