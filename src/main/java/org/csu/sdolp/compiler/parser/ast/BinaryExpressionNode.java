package org.csu.sdolp.compiler.parser.ast;

import org.csu.sdolp.compiler.lexer.Token;

/**
 * AST 节点: 表示一个二元运算表达式 (e.g., age > 20)
 */
public record BinaryExpressionNode(
        ExpressionNode left,
        Token operator,
        ExpressionNode right
) implements ExpressionNode {
}

