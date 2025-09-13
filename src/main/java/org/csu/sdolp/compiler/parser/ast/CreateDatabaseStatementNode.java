package org.csu.sdolp.compiler.parser.ast;

public record CreateDatabaseStatementNode(IdentifierNode databaseName) implements StatementNode {
}