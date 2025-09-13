package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 DROP DATABASE 语句
 * e.g., DROP DATABASE my_database;
 */
public record DropDatabaseStatementNode(
        IdentifierNode databaseName
) implements StatementNode {
}