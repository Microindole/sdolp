package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 LIMIT 子句
 * @param limit 限制返回的行数
 */
public record LimitClauseNode(
        int limit
) implements AstNode {
}