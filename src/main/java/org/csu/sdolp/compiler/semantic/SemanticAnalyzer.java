package org.csu.sdolp.compiler.semantic;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.csu.sdolp.compiler.parser.ast.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hidyouth
 * @description: 语义分析器
 * 负责检查AST的逻辑正确性。
 */
public class SemanticAnalyzer {

    private final Catalog catalog;

    public SemanticAnalyzer(Catalog catalog) {
        this.catalog = catalog;
    }

    public void analyze(AstNode node) {
        if (node instanceof CreateTableStatementNode) {
            analyzeCreateTable((CreateTableStatementNode) node);
        } else if (node instanceof InsertStatementNode) {
            analyzeInsert((InsertStatementNode) node);
        } else if (node instanceof SelectStatementNode) {
            analyzeSelect((SelectStatementNode) node);
        } else if (node instanceof DeleteStatementNode) {
            analyzeDelete((DeleteStatementNode) node);
        } else if (node instanceof UpdateStatementNode) { // <-- 新增分支
            analyzeUpdate((UpdateStatementNode) node);
        }
        else if (node instanceof DropTableStatementNode dropTable) {
            analyzeDropTable(dropTable);
        } else if (node instanceof AlterTableStatementNode alterTable) {
            analyzeAlterTable(alterTable);
        }
        // 可以扩展以支持其他语句类型
    }

    private void analyzeCreateTable(CreateTableStatementNode node) {
        String tableName = node.tableName().getName();
        if (catalog.getTable(tableName) != null) {
            throw new SemanticException("Table '" + tableName + "' already exists.");
        }
        // 检查数据类型是否合法
        for (ColumnDefinitionNode colDef : node.columns()) {
            try {
                DataType.valueOf(colDef.dataType().getName().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new SemanticException("Invalid data type '" + colDef.dataType().getName() + "' for column '" + colDef.columnName().getName() + "'.");
            }
        }
    }

    private void analyzeInsert(InsertStatementNode node) {
        String tableName = node.tableName().getName();
        TableInfo tableInfo = getTableOrThrow(tableName);
        Schema schema = tableInfo.getSchema();

        if (node.columns().size() != node.values().size()) {
            throw new SemanticException("Number of columns does not match number of values.");
        }

        // 检查插入的列是否存在，以及值类型是否匹配
        for (int i = 0; i < node.columns().size(); i++) {
            String colName = node.columns().get(i).getName();
            Column schemaCol = schema.getColumns().stream()
                    .filter(c -> c.getName().equalsIgnoreCase(colName))
                    .findFirst()
                    .orElseThrow(() -> new SemanticException("Column '" + colName + "' does not exist in table '" + tableName + "'."));

            ExpressionNode valueNode = node.values().get(i);
            if (!(valueNode instanceof LiteralNode)) {
                throw new SemanticException("INSERT statements currently only support literal values.");
            }
            LiteralNode literal = (LiteralNode) valueNode;
            DataType expectedType = schemaCol.getType();
            DataType actualType = getLiteralType(literal);

            if (expectedType != actualType) {
                throw new SemanticException("Data type mismatch for column '" + colName + "'. Expected " + expectedType + " but got " + actualType + ".");
            }
        }
    }

    private void analyzeSelect(SelectStatementNode node) {
        String tableName = node.fromTable().getName();
        TableInfo tableInfo = getTableOrThrow(tableName);

        // 如果不是 SELECT *，则检查每一列是否存在
        if (!node.isSelectAll()) {
            for (ExpressionNode expr : node.selectList()) {
                if (expr instanceof IdentifierNode idNode) {
                    // *** 修改点: 检查带限定符的列 ***
                    checkColumnExists(tableInfo, idNode);
                }
            }
        }

        // 检查 WHERE 子句
        if (node.whereClause() != null) {
            analyzeExpression(node.whereClause(), tableInfo);
        }
        // 检查 ORDER BY 子句
        if (node.orderByClause() != null) {
            // *** 修改点: 检查带限定符的列 ***
            checkColumnExists(tableInfo, node.orderByClause().column());
        }
        // LIMIT 子句的值在语法分析时已确认为整数，无需额外语义检查。
        // ============================
    }

    private void analyzeDelete(DeleteStatementNode node) {
        String tableName = node.tableName().getName();
        TableInfo tableInfo = getTableOrThrow(tableName);

        // 检查 WHERE 子句
        if (node.whereClause() != null) {
            analyzeExpression(node.whereClause(), tableInfo);
        }
    }

    private void analyzeUpdate(UpdateStatementNode node) {
        String tableName = node.tableName().getName();
        TableInfo tableInfo = getTableOrThrow(tableName);

        for (SetClauseNode clause : node.setClauses()) {
            // *** 修改点: 检查带限定符的列 ***
            Column column = checkColumnExists(tableInfo, clause.column());
            DataType expectedType = column.getType();
            if (!(clause.value() instanceof LiteralNode literal)) {
                throw new SemanticException("SET clause currently only supports literal values.");
            }
            DataType actualType = getLiteralType(literal);
            if (expectedType != actualType) {
                throw new SemanticException("Data type mismatch for column '" + column.getName() + "'. Expected " + expectedType + " but got " + actualType + ".");
            }
        }
        if (node.whereClause() != null) {
            analyzeExpression(node.whereClause(), tableInfo);
        }
    }

    private void analyzeDropTable(DropTableStatementNode node) {
        // 检查要删除的表是否存在
        getTableOrThrow(node.tableName().getName());
    }

    private void analyzeAlterTable(AlterTableStatementNode node) {
        String tableName = node.tableName().getName();
        TableInfo tableInfo = getTableOrThrow(tableName);

        // 检查要添加的列名是否已存在
        String newColumnName = node.newColumnDefinition().columnName().getName();
        boolean columnExists = tableInfo.getSchema().getColumns().stream()
                .anyMatch(c -> c.getName().equalsIgnoreCase(newColumnName));
        if (columnExists) {
            throw new SemanticException("Column '" + newColumnName + "' already exists in table '" + tableName + "'.");
        }

        // 检查新列的数据类型是否合法
        try {
            DataType.valueOf(node.newColumnDefinition().dataType().getName().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new SemanticException("Invalid data type '" + node.newColumnDefinition().dataType().getName() + "' for new column.");
        }
    }
    private void analyzeExpression(ExpressionNode expr, TableInfo tableInfo) {
        if (expr instanceof BinaryExpressionNode binaryExpr) {
            TokenType opType = binaryExpr.operator().type();
            if (opType == TokenType.AND || opType == TokenType.OR) {
                analyzeExpression(binaryExpr.left(), tableInfo);
                analyzeExpression(binaryExpr.right(), tableInfo);
                return;
            }

            if (!(binaryExpr.left() instanceof IdentifierNode colNode)) {
                throw new SemanticException("WHERE clause must have a column name on the left side of the operator.");
            }
            if (!(binaryExpr.right() instanceof LiteralNode literalNode)) {
                throw new SemanticException("WHERE clause must have a literal value on the right side of the operator.");
            }

            // *** 修改点: 检查带限定符的列 ***
            Column column = checkColumnExists(tableInfo, colNode);

            DataType expectedType = column.getType();
            DataType actualType = getLiteralType(literalNode);
            if(expectedType != actualType) {
                throw new SemanticException("Data type mismatch in WHERE clause for column '" + colNode.getFullName() + "'. Expected " + expectedType + " but got " + actualType + ".");
            }
        } else if (!(expr instanceof IdentifierNode || expr instanceof LiteralNode)) {
            throw new SemanticException("Unsupported expression type in WHERE clause: " + expr.getClass().getSimpleName());
        }
    }

    // --- 辅助方法 ---

    private TableInfo getTableOrThrow(String tableName) {
        TableInfo tableInfo = catalog.getTable(tableName);
        if (tableInfo == null) {
            throw new SemanticException("Table '" + tableName + "' not found.");
        }
        return tableInfo;
    }

    private Column checkColumnExists(TableInfo tableInfo, IdentifierNode columnIdentifier) {
        String tableName = tableInfo.getTableName();
        String columnName = columnIdentifier.getName();
        String qualifier = columnIdentifier.getTableQualifier();

        // 检查表限定符是否匹配
        if (qualifier != null && !qualifier.equalsIgnoreCase(tableName)) {
            throw new SemanticException("Table qualifier '" + qualifier + "' does not match the table '" + tableName + "' in FROM clause.");
        }

        // 在表的 Schema 中查找列
        return tableInfo.getSchema().getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new SemanticException("Column '" + columnName + "' not found in table '" + tableName + "'."));
    }

    private DataType getLiteralType(LiteralNode literal) {
        if (literal.literal().type() == TokenType.INTEGER_CONST) {
            return DataType.INT;
        }
        if (literal.literal().type() == TokenType.STRING_CONST) {
            return DataType.VARCHAR;
        }
        throw new SemanticException("Unsupported literal type: " + literal.literal().type());
    }
}
