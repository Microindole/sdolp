package org.csu.sdolp.compiler.parser.ast.dml;

import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

import java.util.List;

/**
 * AST 节点: 表示 INSERT 语句
 */
public record InsertStatementNode(
        IdentifierNode tableName,
        List<IdentifierNode> columns,
        List<ExpressionNode> values
) implements StatementNode {
}

