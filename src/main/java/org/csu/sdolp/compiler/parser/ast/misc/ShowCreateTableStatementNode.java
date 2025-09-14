package org.csu.sdolp.compiler.parser.ast.misc;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

/**
 * AST 节点: 表示 SHOW CREATE TABLE 语句
 */
public record ShowCreateTableStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}