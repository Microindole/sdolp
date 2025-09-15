package org.csu.sdolp.compiler.parser.ast.dcl;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

import java.util.List;

/**
 * AST 节点: 表示 GRANT 语句
 * e.g., GRANT SELECT, INSERT ON my_table TO 'some_user';
 */
public record GrantStatementNode(
        List<IdentifierNode> privileges,
        IdentifierNode tableName,
        IdentifierNode username
) implements StatementNode {
}