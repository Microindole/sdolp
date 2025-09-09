package org.csu.sdolp.compiler.parser.ast;

public record SetClauseNode(IdentifierNode column, ExpressionNode value) implements AstNode {
}