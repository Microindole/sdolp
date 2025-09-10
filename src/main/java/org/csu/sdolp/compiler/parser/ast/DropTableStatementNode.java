package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 DROP TABLE 语句
 * @param tableName 要删除的表名
 */
public record DropTableStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}
