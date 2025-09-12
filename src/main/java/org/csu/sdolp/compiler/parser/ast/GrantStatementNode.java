package org.csu.sdolp.compiler.parser.ast;

import java.util.List;

/**
 * AST 节点: 表示 GRANT 语句
 * e.g., GRANT SELECT, INSERT ON my_table TO 'someuser';
 */
public record GrantStatementNode(
        List<IdentifierNode> privileges,
        IdentifierNode tableName,
        IdentifierNode username
) implements StatementNode {
}