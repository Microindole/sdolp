package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 ALTER TABLE 语句
 * (目前仅支持 ADD COLUMN)
 * @param tableName 要修改的表名
 * @param newColumnDefinition 新增列的定义
 */
public record AlterTableStatementNode(
        IdentifierNode tableName,
        ColumnDefinitionNode newColumnDefinition
) implements StatementNode {
}
