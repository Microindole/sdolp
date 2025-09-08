package org.csu.sdolp.compiler.parser.ast;

/**
 * @author hdiyouth
 * @description: 用于 CREATE TABLE 语句中的列定义
 */
public record ColumnDefinitionNode(IdentifierNode columnName, IdentifierNode dataType) implements AstNode {
}
