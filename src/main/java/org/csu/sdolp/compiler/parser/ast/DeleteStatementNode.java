package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 DELETE 语句
 */
public record DeleteStatementNode(
        IdentifierNode tableName,
        ExpressionNode whereClause
) implements StatementNode {
}

