package org.csu.sdolp.compiler.parser.ast;

import java.util.List;

/**
 * @author hidyouth
 * @description: 表示一个 SELECT 语句
 * 注意：为了简化，我们暂时不支持 WHERE 子句，可以在之后迭代添加
 */
public record SelectStatementNode(
        List<ExpressionNode> selectList,
        IdentifierNode fromTable
) implements StatementNode {
}
