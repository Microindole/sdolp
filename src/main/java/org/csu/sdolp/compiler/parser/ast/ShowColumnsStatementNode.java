package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 SHOW COLUMNS 语句
 * e.g., SHOW COLUMNS FROM my_table;
 */
public record ShowColumnsStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}