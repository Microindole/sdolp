package org.csu.sdolp.compiler.planner;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.IndexInfo;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Column;
import org.csu.sdolp.common.model.DataType;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.lexer.TokenType;
import org.csu.sdolp.compiler.parser.ast.*;
import org.csu.sdolp.compiler.planner.plan.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hidyouth
 * @description: 执行计划生成器
 * 负责将经过语义分析的AST转换为物理执行计划
 */
public class Planner {

    private final Catalog catalog;

    public Planner(Catalog catalog) {
        this.catalog = catalog;
    }

    public PlanNode createPlan(StatementNode ast) {
        if (ast instanceof CreateTableStatementNode stmt) {
            return createTablePlan(stmt);
        }
        if (ast instanceof InsertStatementNode stmt) {
            return createInsertPlan(stmt);
        }
        if (ast instanceof SelectStatementNode stmt) {
            return createSelectPlan(stmt);
        }
        if (ast instanceof DeleteStatementNode stmt) {
            return createDeletePlan(stmt);
        }
        if (ast instanceof UpdateStatementNode stmt) {
            return createUpdatePlan(stmt);
        }
        if (ast instanceof DropTableStatementNode stmt) {
            return createDropTablePlan(stmt);
        }
        if (ast instanceof AlterTableStatementNode stmt) {
            return createAlterTablePlan(stmt);
        }
        if (ast instanceof CreateIndexStatementNode stmt) {
            return createIndexPlan(stmt);
        }
        if (ast instanceof CreateUserStatementNode stmt) {
            return new CreateUserPlanNode(stmt.username(), stmt.password());
        }
        if (ast instanceof GrantStatementNode stmt) {
            return new GrantPlanNode(stmt.privileges(), stmt.tableName(), stmt.username());
        }

        throw new UnsupportedOperationException("Unsupported statement type for planning: " + ast.getClass().getSimpleName());
    }

    private PlanNode createTablePlan(CreateTableStatementNode ast) {
        String tableName = ast.tableName().getName();
        List<Column> columns = ast.columns().stream()
                .map(colDef -> new Column(
                        colDef.columnName().getName(),
                        DataType.valueOf(colDef.dataType().getName().toUpperCase())))
                .collect(Collectors.toList());
        Schema schema = new Schema(columns);
        return new CreateTablePlanNode(tableName, schema);
    }

    private PlanNode createInsertPlan(InsertStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        List<Value> values = new ArrayList<>();
        for (ExpressionNode expr : ast.values()) {
            if (expr instanceof LiteralNode literal) {
                if (literal.literal().type() == TokenType.INTEGER_CONST) {
                    values.add(new Value(Integer.parseInt(literal.literal().lexeme())));
                } else if (literal.literal().type() == TokenType.STRING_CONST) {
                    values.add(new Value(literal.literal().lexeme()));
                }
            }
        }
        Tuple tuple = new Tuple(values);
        return new InsertPlanNode(tableInfo, List.of(tuple));
    }

    /**
     * 新增：为 CREATE INDEX 语句创建执行计划。
     * @param ast CreateIndexStatementNode
     * @return CreateIndexPlanNode
     */
    private PlanNode createIndexPlan(CreateIndexStatementNode ast) {
        String indexName = ast.getIndexName().getName();
        String tableName = ast.getTableName().getName();
        // 假设AST中只有一个列用于索引
        String columnName = ast.getColumnNames().get(0).getName();

        TableInfo tableInfo = catalog.getTable(tableName);
        if (tableInfo == null) {
            throw new IllegalStateException("Table '" + tableName + "' not found for index creation.");
        }
        return new CreateIndexPlanNode(indexName, tableName, columnName, tableInfo);
    }


    /**
     * 【最终版本】创建 SELECT 查询计划，完整集成了索引选择和您原有的 JOIN 等逻辑。
     */
    private PlanNode createSelectPlan(SelectStatementNode ast) {
        String fromTableName = ast.fromTable().getName();
        TableInfo fromTableInfo = catalog.getTable(fromTableName);
        PlanNode plan;

        IndexInfo indexInfo = findIndexForPredicate(fromTableName, ast.whereClause());

        if (indexInfo != null) {
            System.out.println("[Planner] Index found for '" + fromTableName + "." + indexInfo.getColumnName() + "'. Using Index Scan.");
            Value searchValue = extractValueFromPredicate(ast.whereClause());
            plan = new IndexScanPlanNode(fromTableInfo, indexInfo, searchValue);
        } else {
            System.out.println("[Planner] No suitable index found for query. Using Sequential Scan.");
            plan = new SeqScanPlanNode(fromTableInfo);
            if (ast.whereClause() != null) {
                plan = new FilterPlanNode(plan, ast.whereClause());
            }
        }

        if (ast.joinTable() != null) {
            TableInfo rightTableInfo = catalog.getTable(ast.joinTable().getName());
            PlanNode rightPlan = new SeqScanPlanNode(rightTableInfo);
            plan = new JoinPlanNode(plan, rightPlan, ast.joinCondition());
        }

        if (!ast.isSelectAll()) {
            Schema inputSchemaForProject = plan.getOutputSchema();

            List<Column> projectedColumns = new ArrayList<>();
            for (ExpressionNode expr : ast.selectList()) {
                if (expr instanceof IdentifierNode idNode) {
                    Column originalColumn = findColumnInSchema(inputSchemaForProject, idNode.getName());
                    projectedColumns.add(originalColumn);
                }
            }
            Schema projectedSchema = new Schema(projectedColumns);
            plan = new ProjectPlanNode(plan, projectedSchema);
        }

        if (ast.orderByClause() != null) {
            plan = new SortPlanNode(plan, ast.orderByClause());
        }
        if (ast.limitClause() != null) {
            plan = new LimitPlanNode(plan, ast.limitClause().limit());
        }

        return plan;
    }

    private PlanNode createDeletePlan(DeleteStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        PlanNode childPlan = new SeqScanPlanNode(tableInfo);
        if (ast.whereClause() != null) {
            childPlan = new FilterPlanNode(childPlan, ast.whereClause());
        }
        return new DeletePlanNode(childPlan, tableInfo);
    }

    private PlanNode createUpdatePlan(UpdateStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        PlanNode childPlan = new SeqScanPlanNode(tableInfo);
        if (ast.whereClause() != null) {
            childPlan = new FilterPlanNode(childPlan, ast.whereClause());
        }
        return new UpdatePlanNode(childPlan, tableInfo, ast.setClauses());
    }

    private PlanNode createDropTablePlan(DropTableStatementNode ast) {
        return new DropTablePlanNode(ast.tableName().getName());
    }

    private PlanNode createAlterTablePlan(AlterTableStatementNode ast) {
        ColumnDefinitionNode colDef = ast.newColumnDefinition();
        Column newColumn = new Column(
                colDef.columnName().getName(),
                DataType.valueOf(colDef.dataType().getName().toUpperCase())
        );
        return new AlterTablePlanNode(ast.tableName().getName(), newColumn);
    }

    private Column findColumnInSchema(Schema schema, String columnName) {
        return schema.getColumns().stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Column '" + columnName + "' not found in plan's output schema. " +
                                "This should have been caught during semantic analysis."
                ));
    }

    private IndexInfo findIndexForPredicate(String tableName, ExpressionNode predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof BinaryExpressionNode binaryExpr) {
            if (binaryExpr.operator().type() == TokenType.EQUAL &&
                    binaryExpr.left() instanceof IdentifierNode &&
                    binaryExpr.right() instanceof LiteralNode) {

                String columnName = ((IdentifierNode) binaryExpr.left()).getName();
                return catalog.getIndex(tableName, columnName);
            }
        }
        return null;
    }

    private Value extractValueFromPredicate(ExpressionNode predicate) {
        if (predicate instanceof BinaryExpressionNode binaryExpr && binaryExpr.right() instanceof LiteralNode literal) {
            if (literal.literal().type() == TokenType.INTEGER_CONST) {
                return new Value(Integer.parseInt(literal.literal().lexeme()));
            } else if (literal.literal().type() == TokenType.STRING_CONST) {
                return new Value(literal.literal().lexeme());
            }
        }
        throw new IllegalStateException("Could not extract value from predicate for index scan. This indicates a planner bug.");
    }
}