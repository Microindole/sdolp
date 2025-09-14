package org.csu.sdolp.compiler.parser.ast.expression;

import org.csu.sdolp.compiler.parser.ast.AstNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;

public record SetClauseNode(IdentifierNode column, ExpressionNode value) implements AstNode {
}