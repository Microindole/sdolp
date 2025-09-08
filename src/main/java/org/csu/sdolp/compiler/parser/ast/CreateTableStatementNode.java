package org.csu.sdolp.compiler.parser.ast;

import java.util.List;

/**
 * @author hidyouth
 * @description: 表示一个 CREATE TABLE 语句
 */
public record CreateTableStatementNode(
        IdentifierNode tableName,
        List<ColumnDefinitionNode> columns
) implements StatementNode {
}
