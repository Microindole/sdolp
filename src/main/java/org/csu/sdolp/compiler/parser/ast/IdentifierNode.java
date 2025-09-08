package org.csu.sdolp.compiler.parser.ast;

/**
 * @author hidyouth
 * @description: 表示一个标识符，如表名或列名
 */
public record IdentifierNode(String name) implements ExpressionNode {
}
