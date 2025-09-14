package org.csu.sdolp.compiler.parser.ast.ddl;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

public record CreateDatabaseStatementNode(IdentifierNode databaseName) implements StatementNode {
}