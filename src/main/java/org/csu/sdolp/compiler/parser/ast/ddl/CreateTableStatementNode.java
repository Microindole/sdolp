package org.csu.sdolp.compiler.parser.ast.ddl;

import org.csu.sdolp.compiler.parser.ast.expression.ColumnDefinitionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

import java.util.List;

/**
 * @author hidyouth
 * @description: 表示一个 CREATE TABLE 语句
 */
public record CreateTableStatementNode(
        IdentifierNode tableName,
        List<ColumnDefinitionNode> columns,
        IdentifierNode primaryKeyColumn
) implements StatementNode {
}
