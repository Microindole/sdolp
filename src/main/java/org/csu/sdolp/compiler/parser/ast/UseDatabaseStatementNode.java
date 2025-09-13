package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 USE database 语句
 */
public record UseDatabaseStatementNode(
        IdentifierNode databaseName
) implements StatementNode {
}