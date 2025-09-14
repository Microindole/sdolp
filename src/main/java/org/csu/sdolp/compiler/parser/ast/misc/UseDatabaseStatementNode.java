package org.csu.sdolp.compiler.parser.ast.misc;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

/**
 * AST 节点: 表示 USE database 语句
 */
public record UseDatabaseStatementNode(
        IdentifierNode databaseName
) implements StatementNode {
}