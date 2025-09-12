package org.csu.sdolp.compiler.planner;

import org.csu.sdolp.catalog.Catalog;
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
        if (ast instanceof DeleteStatementNode) {
            return createDeletePlan((DeleteStatementNode) ast);
        }
        if (ast instanceof UpdateStatementNode) {
            return createUpdatePlan((UpdateStatementNode) ast);
        }
        if (ast instanceof DropTableStatementNode stmt) {
            return createDropTablePlan(stmt);
        }
        if (ast instanceof AlterTableStatementNode stmt) {
            return createAlterTablePlan(stmt);
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

    // ====== (Phase 4) ======
    private PlanNode createSelectPlan(SelectStatementNode ast) {
        TableInfo leftTableInfo = catalog.getTable(ast.fromTable().getName());
        PlanNode plan = new SeqScanPlanNode(leftTableInfo);

        // 2. 如果有 JOIN，创建 JoinPlanNode
        if (ast.joinTable() != null) {
            TableInfo rightTableInfo = catalog.getTable(ast.joinTable().getName());
            PlanNode rightPlan = new SeqScanPlanNode(rightTableInfo);
            plan = new JoinPlanNode(plan, rightPlan, ast.joinCondition());
        }

        // 3. WHERE 子句 -> Filter
        if (ast.whereClause() != null) {
            plan = new FilterPlanNode(plan, ast.whereClause());
        }

        // --- 聚合和分组逻辑 ---
        /*boolean hasGroupBy = ast.groupByClause() != null && !ast.groupByClause().isEmpty();
        List<AggregateExpressionNode> aggregates = ast.selectList().stream()
                .filter(AggregateExpressionNode.class::isInstance)
                .map(AggregateExpressionNode.class::cast)
                .collect(Collectors.toList());
        boolean hasAggregate = !aggregates.isEmpty();

        if (hasGroupBy || hasAggregate) {
            // 构造聚合后的 Schema
            List<Column> outputColumns = new ArrayList<>();
            if (hasGroupBy) {
                for (IdentifierNode groupByColId : ast.groupByClause()) {
                    Column originalCol = tableInfo.getSchema().getColumn(groupByColId.getName());
                    outputColumns.add(new Column(originalCol.getName(), originalCol.getType()));
                }
            }
            for (AggregateExpressionNode agg : aggregates) {
                // 聚合结果通常是 INT 或 DECIMAL，这里简化为INT
                outputColumns.add(new Column(agg.functionName(), DataType.INT));
            }
            Schema aggSchema = new Schema(outputColumns);

            plan = new AggregatePlanNode(plan, ast.groupByClause(), aggregates, aggSchema);
        }*/

        // --- 投影逻辑 ---
        if (!ast.isSelectAll()) {
            // 【修正点】投影逻辑不应该被 if 包裹，它总是需要执行（除非是 SELECT *）
            // 从当前 plan (可能是 Scan, Join, 或 Aggregate) 的输出中选择列
            Schema inputSchemaForProject = plan.getOutputSchema();

            List<Column> projectedColumns = new ArrayList<>();
            for (ExpressionNode expr : ast.selectList()) {
                if (expr instanceof IdentifierNode idNode) {
                    Column originalColumn = findColumnInSchema(inputSchemaForProject, idNode.getName());
                    projectedColumns.add(originalColumn);
                    // 假设您有 AggregateExpressionNode
                } /* else if (expr instanceof AggregateExpressionNode aggNode) {
                    // 从聚合节点的输出中找到对应的列
                    Column aggColumn = findColumnInSchema(inputSchemaForProject, aggNode.toString());
                    projectedColumns.add(aggColumn);
                }*/
            }
            Schema projectedSchema = new Schema(projectedColumns);
            plan = new ProjectPlanNode(plan, projectedSchema);
        }

        // --- 6. 排序和限制 ---
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
        // 1. 创建一个子计划来找到所有要删除的元组
        PlanNode childPlan = new SeqScanPlanNode(tableInfo);
        if (ast.whereClause() != null) {
            childPlan = new FilterPlanNode(childPlan, ast.whereClause());
        }
        // 2. 用 DeletePlanNode 包装子计划
        return new DeletePlanNode(childPlan, tableInfo);
    }

    private PlanNode createUpdatePlan(UpdateStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        // 1. 创建一个子计划来找到所有要更新的元组
        PlanNode childPlan = new SeqScanPlanNode(tableInfo);
        if (ast.whereClause() != null) {
            childPlan = new FilterPlanNode(childPlan, ast.whereClause());
        }
        // 2. 用 UpdatePlanNode 包装子计划
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
}
