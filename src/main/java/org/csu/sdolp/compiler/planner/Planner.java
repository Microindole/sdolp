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

    private PlanNode createSelectPlan(SelectStatementNode ast) {
        // 1. FROM 子句 -> SeqScan
        TableInfo tableInfo = catalog.getTable(ast.fromTable().getName());
        PlanNode plan = new SeqScanPlanNode(tableInfo);

        // 2. WHERE 子句 -> Filter
        if (ast.whereClause() != null) {
            plan = new FilterPlanNode(plan, ast.whereClause());
        }

        // 3. SELECT 子句 -> Project
        if (ast.isSelectAll()) {
            // SELECT * 的情况，输出 Schema 就是表的原始 Schema
            // 所以 SeqScan 或 Filter 的输出 Schema 已经是正确的，无需再加 ProjectNode
            return plan;
        } else {
            // 构造投影后的 Schema
            List<Column> projectedColumns = new ArrayList<>();
            for (ExpressionNode expr : ast.selectList()) {
                if (expr instanceof IdentifierNode idNode) {
                    // 从表的原始 Schema 中找到对应的列
                    Column originalColumn = tableInfo.getSchema().getColumns().stream()
                            .filter(c -> c.getName().equalsIgnoreCase(idNode.getName()))
                            .findFirst().orElseThrow(); // 语义分析已保证列存在
                    projectedColumns.add(originalColumn);
                }
            }
            Schema projectedSchema = new Schema(projectedColumns);
            plan = new ProjectPlanNode(plan, projectedSchema);
        }
        // 4. ORDER BY 子句 -> Sort
        if (ast.orderByClause() != null) {
            plan = new SortPlanNode(plan, ast.orderByClause());
        }

        // 5. LIMIT 子句 -> Limit
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
}
