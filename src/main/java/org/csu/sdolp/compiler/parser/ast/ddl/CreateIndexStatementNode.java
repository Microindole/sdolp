package org.csu.sdolp.compiler.parser.ast.ddl;

import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.StatementNode;

import java.util.List;

/**
 * 代表 "CREATE INDEX" 语句的抽象语法树（AST）节点。
 */
public class CreateIndexStatementNode implements StatementNode {

    private final IdentifierNode indexName;
    private final IdentifierNode tableName;
    private final List<IdentifierNode> columnNames;

    public CreateIndexStatementNode(IdentifierNode indexName, IdentifierNode tableName, List<IdentifierNode> columnNames) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public IdentifierNode getIndexName() {
        return indexName;
    }

    public IdentifierNode getTableName() {
        return tableName;
    }

    public List<IdentifierNode> getColumnNames() {
        return columnNames;
    }
}