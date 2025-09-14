package org.csu.sdolp.compiler.parser.ast.dml;

import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.expression.SetClauseNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

import java.util.List;


public record UpdateStatementNode(
        IdentifierNode tableName,
        List<SetClauseNode> setClauses,
        ExpressionNode whereClause
) implements StatementNode {
}