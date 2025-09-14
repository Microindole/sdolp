package org.csu.sdolp.compiler.parser.ast.expression;

import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;

/**
 * AST 节点: 表示一个字面量 (如数字、字符串)
 */
public record LiteralNode(Token literal) implements ExpressionNode {
}

