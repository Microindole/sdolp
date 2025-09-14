package org.csu.sdolp.compiler.parser.ast.expression;

import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;

/**
 * AST 节点: 表示一个二元运算表达式 (e.g., age > 20)
 */
public record BinaryExpressionNode(
        ExpressionNode left,
        Token operator,
        ExpressionNode right
) implements ExpressionNode {
}

