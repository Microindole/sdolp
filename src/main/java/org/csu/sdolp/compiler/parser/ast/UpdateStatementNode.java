package org.csu.sdolp.compiler.parser.ast;

import java.util.List;


public record UpdateStatementNode(
        IdentifierNode tableName,
        List<SetClauseNode> setClauses,
        ExpressionNode whereClause
) implements StatementNode {
}