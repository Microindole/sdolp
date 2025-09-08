package org.csu.sdolp.compiler.parser.ast;

import org.csu.sdolp.compiler.lexer.Token;

/**
 * AST 节点: 表示一个字面量 (如数字、字符串)
 */
public record LiteralNode(Token literal) implements ExpressionNode {
}

