package org.csu.sdolp.compiler.parser.ast.expression;

import org.csu.sdolp.compiler.parser.ast.AstNode;

/**
 * AST 节点: 表示 ORDER BY 子句
 * @param column 要排序的列
 * @param isAscending 是否为升序 (true for ASC, false for DESC)
 */
public record OrderByClauseNode(
        IdentifierNode column,
        boolean isAscending
) implements AstNode {
}