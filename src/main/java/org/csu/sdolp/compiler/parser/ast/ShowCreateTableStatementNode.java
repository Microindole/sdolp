package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 SHOW CREATE TABLE 语句
 */
public record ShowCreateTableStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}