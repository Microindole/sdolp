package org.csu.sdolp.compiler.parser.ast;

/**
 * AST 节点: 表示 CREATE USER 语句
 * e.g., CREATE USER 'newuser' IDENTIFIED BY 'password123';
 */
public record CreateUserStatementNode(
        IdentifierNode username,
        LiteralNode password
) implements StatementNode {
}