package org.csu.sdolp.compiler.parser.ast.ddl;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

/**
 * AST 节点: 表示 DROP TABLE 语句
 * @param tableName 要删除的表名
 */
public record DropTableStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}
