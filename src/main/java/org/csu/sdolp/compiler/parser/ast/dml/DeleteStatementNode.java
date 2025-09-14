package org.csu.sdolp.compiler.parser.ast.dml;

import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

/**
 * AST 节点: 表示 DELETE 语句
 */
public record DeleteStatementNode(
        IdentifierNode tableName,
        ExpressionNode whereClause
) implements StatementNode {
}

