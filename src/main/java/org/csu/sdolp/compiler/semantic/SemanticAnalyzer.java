package org.csu.sdolp.compiler.semantic;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.cli.Session;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.csu.sdolp.compiler.parser.ast.*;

import java.util.HashSet;
import java.util.Set;

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

    public void analyze(AstNode node, Session session) {
        if (session == null || !session.isAuthenticated()) {
            throw new SemanticException("Access denied. User is not authenticated.");
        }

        if (node instanceof CreateTableStatementNode) {
            analyzeCreateTable((CreateTableStatementNode) node, session);
        } else if (node instanceof InsertStatementNode) {
            analyzeInsert((InsertStatementNode) node, session);
        } else if (node instanceof SelectStatementNode) {
            analyzeSelect((SelectStatementNode) node, session);
        } else if (node instanceof DeleteStatementNode) {
            analyzeDelete((DeleteStatementNode) node, session);
        } else if (node instanceof UpdateStatementNode) {
            analyzeUpdate((UpdateStatementNode) node, session);
        } else if (node instanceof DropTableStatementNode dropTable) {
            analyzeDropTable(dropTable, session);
        } else if (node instanceof AlterTableStatementNode alterTable) {
            analyzeAlterTable(alterTable, session);
        }
    }

    private void analyzeCreateTable(CreateTableStatementNode node,Session session) {
        if (!"root".equalsIgnoreCase(session.getUsername())) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. CREATE TABLE privilege required.");
        }
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

    private void analyzeInsert(InsertStatementNode node,Session session) {
        String tableName = node.tableName().getName();
        if (!catalog.hasPermission(session.getUsername(), tableName, "INSERT")) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. INSERT command denied on table '" + tableName + "'.");
        }
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

    // ====== (Phase 4) ======
    private void analyzeSelect(SelectStatementNode node,Session session) {
        String tableName = node.fromTable().getName();
        // 权限检查
        if (!catalog.hasPermission(session.getUsername(), tableName, "SELECT")) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. SELECT command denied on table '" + tableName + "'.");
        }
        // 1. 检查 FROM 表和 JOIN 表
        TableInfo leftTableInfo = getTableOrThrow(node.fromTable().getName());
        TableInfo rightTableInfo = null;

        // 如果有 JOIN，检查 JOIN 的表并验证连接条件
        if (node.joinTable() != null) {
            rightTableInfo = getTableOrThrow(node.joinTable().getName());
            if (node.joinCondition() != null) {
                // 传入两个表的 schema 用于检查 ON 子句中的列
                analyzeJoinExpression(node.joinCondition(), leftTableInfo, rightTableInfo);
            } else {
                throw new SemanticException("JOIN clause requires an ON condition.");
            }
        }

        Set<String> groupByColumnNames = new HashSet<>();
        if (!node.isSelectAll()) {
            for (ExpressionNode expr : node.selectList()) {
                if (expr instanceof IdentifierNode idNode) {
                    // **【修正点】** 使用新的辅助方法检查列
                    checkColumnExistsInJoinedTables(leftTableInfo, rightTableInfo, idNode);
                    // 如果有 GROUP BY，那么普通列必须是分组键的一部分
                    if (!groupByColumnNames.isEmpty() && !groupByColumnNames.contains(idNode.getName().toLowerCase())) {
                        throw new SemanticException("Column '" + idNode.getFullName() + "' must appear in the GROUP BY clause or be used in an aggregate function.");
                    }
                    // 假设您有 AggregateExpressionNode
                }/* else if (expr instanceof AggregateExpressionNode aggNode) {
                    // 检查聚合函数的参数
                    if (!aggNode.isStar()) {
                        if (aggNode.argument() instanceof IdentifierNode argId) {
                            checkColumnExistsInJoinedTables(leftTableInfo, rightTableInfo, argId);
                        } else {
                            throw new SemanticException("Aggregate function argument must be a column identifier.");
                        }
                    } else {
                        // COUNT(*) 的特殊情况检查
                        if (!aggNode.functionName().equalsIgnoreCase("COUNT")) {
                            throw new SemanticException("The '*' argument is only valid for the COUNT function.");
                        }
                    }
                }*/
            }
        } else if (!groupByColumnNames.isEmpty()) {
            // 如果是 SELECT * 但有 GROUP BY，这是不合法的 (标准SQL)
            throw new SemanticException("SELECT * is not allowed with GROUP BY clause.");
        }

        // 4. 检查 WHERE 子句
        if (node.whereClause() != null) {
            // **【修正点】** 使用能处理两个表的 analyzeWhereOrJoinExpression 方法
            analyzeWhereOrJoinExpression(node.whereClause(), leftTableInfo, rightTableInfo);
        }

        // 5. 检查 ORDER BY 子句
        if (node.orderByClause() != null) {
            // **【修正点】** 使用新的辅助方法检查列
            checkColumnExistsInJoinedTables(leftTableInfo, rightTableInfo, node.orderByClause().column());
        }
    }


    private void analyzeDelete(DeleteStatementNode node,Session session) {
        String tableName = node.tableName().getName();
        // 权限检查
        if (!catalog.hasPermission(session.getUsername(), tableName, "DELETE")) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. DELETE command denied on table '" + tableName + "'.");
        }
        TableInfo tableInfo = getTableOrThrow(tableName);

        // 检查 WHERE 子句
        if (node.whereClause() != null) {
            analyzeExpression(node.whereClause(), tableInfo);
        }
    }

    private void analyzeUpdate(UpdateStatementNode node,Session session) {
        String tableName = node.tableName().getName();
        // 权限检查
        if (!catalog.hasPermission(session.getUsername(), tableName, "UPDATE")) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. UPDATE command denied on table '" + tableName + "'.");
        }
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

    private void analyzeDropTable(DropTableStatementNode node,Session session) {
        // 只有 root 可以删表
        if (!"root".equalsIgnoreCase(session.getUsername())) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. DROP TABLE privilege required.");
        }
        // 检查要删除的表是否存在
        getTableOrThrow(node.tableName().getName());
    }

    private void analyzeAlterTable(AlterTableStatementNode node,Session session) {
        // 只有 root 可以改表结构
        if (!"root".equalsIgnoreCase(session.getUsername())) {
            throw new SemanticException("Access denied for user '" + session.getUsername() + "'. ALTER TABLE privilege required.");
        }
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
        analyzeWhereOrJoinExpression(expr, tableInfo, null);
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

    private void analyzeWhereOrJoinExpression(ExpressionNode expr, TableInfo leftTable, TableInfo rightTable) {
        if (expr instanceof BinaryExpressionNode binaryExpr) {
            TokenType opType = binaryExpr.operator().type();
            // 递归处理 AND/OR
            if (opType == TokenType.AND || opType == TokenType.OR) {
                analyzeWhereOrJoinExpression(binaryExpr.left(), leftTable, rightTable);
                analyzeWhereOrJoinExpression(binaryExpr.right(), leftTable, rightTable);
                return;
            }

            // 处理 col op literal (常见于 WHERE)
            if (binaryExpr.left() instanceof IdentifierNode colNode && binaryExpr.right() instanceof LiteralNode literalNode) {
                Column column = checkColumnExistsInJoinedTables(leftTable, rightTable, colNode);
                DataType expectedType = column.getType();
                DataType actualType = getLiteralType(literalNode);
                if (expectedType != actualType) {
                    throw new SemanticException("Data type mismatch for column '" + colNode.getFullName() + "'. Expected " + expectedType + " but got " + actualType + ".");
                }
                // 处理 col op col (常见于 JOIN ON)
            } else if (binaryExpr.left() instanceof IdentifierNode leftCol && binaryExpr.right() instanceof IdentifierNode rightCol) {
                checkColumnExistsInJoinedTables(leftTable, rightTable, leftCol);
                checkColumnExistsInJoinedTables(leftTable, rightTable, rightCol);
            } else {
                throw new SemanticException("Unsupported expression format in WHERE or ON clause.");
            }
        }
    }


    private void analyzeJoinExpression(ExpressionNode expr, TableInfo leftTable, TableInfo rightTable) {
        if (expr instanceof BinaryExpressionNode binaryExpr) {
            // 确保是 column = column 的形式
            if (!(binaryExpr.left() instanceof IdentifierNode) || !(binaryExpr.right() instanceof IdentifierNode)) {
                throw new SemanticException("JOIN ON condition must be in the format 'table1.column1 = table2.column2'.");
            }
            checkColumnExistsInJoinedTables(leftTable, rightTable, (IdentifierNode) binaryExpr.left());
            checkColumnExistsInJoinedTables(leftTable, rightTable, (IdentifierNode) binaryExpr.right());
        } else {
            throw new SemanticException("Unsupported JOIN condition expression.");
        }
    }

    private Column checkColumnExistsInJoinedTables(TableInfo left, TableInfo right, IdentifierNode columnIdentifier) {
        String columnName = columnIdentifier.getName();
        String qualifier = columnIdentifier.getTableQualifier();

        // 如果有表限定符 (e.g., users.id)
        if (qualifier != null) {
            if (qualifier.equalsIgnoreCase(left.getTableName())) {
                return checkColumnExists(left, columnIdentifier);
            }
            if (right != null && qualifier.equalsIgnoreCase(right.getTableName())) {
                return checkColumnExists(right, columnIdentifier);
            }
            throw new SemanticException("Table qualifier '" + qualifier + "' not found in FROM or JOIN clause.");
        }

        // 如果没有表限定符 (e.g., id)
        boolean inLeft = left.getSchema().getColumns().stream().anyMatch(c -> c.getName().equalsIgnoreCase(columnName));
        boolean inRight = right != null && right.getSchema().getColumns().stream().anyMatch(c -> c.getName().equalsIgnoreCase(columnName));

        if (inLeft && inRight) {
            throw new SemanticException("Column '" + columnName + "' is ambiguous; it exists in both tables. Please use a table qualifier (e.g., '" + left.getTableName() + "." + columnName + "').");
        }

        if (inLeft) {
            return checkColumnExists(left, columnIdentifier);
        }

        if (inRight) {
            return checkColumnExists(right, columnIdentifier);
        }

        throw new SemanticException("Column '" + columnName + "' not found in any specified table.");
    }
}
