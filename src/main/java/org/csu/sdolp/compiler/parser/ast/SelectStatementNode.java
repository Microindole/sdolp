package org.csu.sdolp.compiler.parser.ast;

import java.util.List;

/**
 * @author hidyouth
 * @description: 表示一个 SELECT 语句
 *
 * @param selectList  查询的列或表达式列表
 * @param fromTable   查询的表
 * @param whereClause WHERE 条件子句 (可以为 null)
 * @param isSelectAll 是否为 SELECT *
 */
public record SelectStatementNode(
        List<ExpressionNode> selectList,
        IdentifierNode fromTable,
        ExpressionNode whereClause,
        boolean isSelectAll,
        List<IdentifierNode> groupByClause,
        OrderByClauseNode orderByClause,
        LimitClauseNode limitClause
) implements StatementNode {
}

