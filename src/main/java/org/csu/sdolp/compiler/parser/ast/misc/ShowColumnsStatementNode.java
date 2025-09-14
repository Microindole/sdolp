package org.csu.sdolp.compiler.parser.ast.misc;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

/**
 * AST 节点: 表示 SHOW COLUMNS 语句
 * e.g., SHOW COLUMNS FROM my_table;
 */
public record ShowColumnsStatementNode(
        IdentifierNode tableName
) implements StatementNode {
}