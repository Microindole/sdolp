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
import org.csu.sdolp.compiler.parser.ast.dcl.CreateUserStatementNode;
import org.csu.sdolp.compiler.parser.ast.dcl.GrantStatementNode;
import org.csu.sdolp.compiler.parser.ast.ddl.*;
import org.csu.sdolp.compiler.parser.ast.dml.DeleteStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.InsertStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.SelectStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.UpdateStatementNode;
import org.csu.sdolp.compiler.parser.ast.expression.*;
import org.csu.sdolp.compiler.parser.ast.misc.*;
import org.csu.sdolp.compiler.planner.plan.*;
import org.csu.sdolp.compiler.planner.plan.dcl.CreateUserPlanNode;
import org.csu.sdolp.compiler.planner.plan.dcl.GrantPlanNode;
import org.csu.sdolp.compiler.planner.plan.ddl.*;
import org.csu.sdolp.compiler.planner.plan.dml.DeletePlanNode;
import org.csu.sdolp.compiler.planner.plan.dml.InsertPlanNode;
import org.csu.sdolp.compiler.planner.plan.dml.UpdatePlanNode;
import org.csu.sdolp.compiler.planner.plan.query.*;
import org.csu.sdolp.compiler.planner.plan.show.ShowColumnsPlanNode;
import org.csu.sdolp.compiler.planner.plan.show.ShowCreateTablePlanNode;
import org.csu.sdolp.compiler.planner.plan.show.ShowDatabasesPlanNode;
import org.csu.sdolp.compiler.planner.plan.show.ShowTablesPlanNode;

import java.math.BigDecimal;
import java.time.LocalDate;
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
        if (ast instanceof CreateDatabaseStatementNode stmt) {
            return new CreateDatabasePlanNode(stmt.databaseName().getName());
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
        if (ast instanceof DropDatabaseStatementNode stmt) {
            return new DropDatabasePlanNode(stmt.databaseName().getName());
        }
        if (ast instanceof AlterTableStatementNode stmt) {
            return createAlterTablePlan(stmt);
        }
        if (ast instanceof ShowTablesStatementNode stmt) {
            return new ShowTablesPlanNode();
        }
        if (ast instanceof ShowColumnsStatementNode stmt) {
            return new ShowColumnsPlanNode(stmt.tableName().getName());
        }
        if (ast instanceof ShowCreateTableStatementNode stmt) {
            return new ShowCreateTablePlanNode(stmt.tableName().getName());
        }
        if (ast instanceof ShowDatabasesStatementNode) {
            return new ShowDatabasesPlanNode();
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
        if (ast instanceof UseDatabaseStatementNode stmt) {
            return null; // USE语句不需要计划节点，在QueryProcessor中直接处理
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
        Schema schema = new Schema(columns, ast.primaryKeyColumn() != null ? ast.primaryKeyColumn().getName() : null);
        return new CreateTablePlanNode(tableName, schema);
    }

    private PlanNode createInsertPlan(InsertStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        Schema schema = tableInfo.getSchema();
        List<Value> values = new ArrayList<>();

        // --- 核心修改：根据 Schema 类型来解析字面量 ---
        for (int i = 0; i < ast.values().size(); i++) {
            ExpressionNode expr = ast.values().get(i);
            // 找到此位置对应的列定义
            String colName = ast.columns().get(i).getName();
            Column column = schema.getColumn(colName);
            DataType expectedType = column.getType();

            if (expr instanceof LiteralNode literal) {
                String lexeme = literal.literal().lexeme();
                TokenType tokenType = literal.literal().type();

                switch (expectedType) {
                    case INT:
                        values.add(new Value(Integer.parseInt(lexeme)));
                        break;
                    case VARCHAR:
                        values.add(new Value(lexeme));
                        break;
                    case DECIMAL:
                        values.add(new Value(new BigDecimal(lexeme)));
                        break;
                    case DATE:
                        values.add(new Value(LocalDate.parse(lexeme)));
                        break;
                    case BOOLEAN:
                        values.add(new Value(tokenType == TokenType.TRUE));
                        break;
                    case FLOAT: //
                        values.add(new Value(Float.parseFloat(lexeme)));
                        break;
                    case DOUBLE: //
                        values.add(new Value(Double.parseDouble(lexeme)));
                        break;
                    case CHAR: //
                        // 使用可以指定类型的构造函数
                        values.add(new Value(DataType.CHAR, lexeme));
                        break;
                    default:
                        throw new IllegalStateException("Unsupported data type in planner for INSERT: " + expectedType);
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
     * 【已修复版本】创建 SELECT 查询计划的逻辑
     */
    private PlanNode createSelectPlan(SelectStatementNode ast) {
        //基础扫描层
        TableInfo fromTableInfo = catalog.getTable(ast.fromTable().getName());
        PlanNode plan;
        // 检查是否有 WHERE 子句，以及是否能找到合适的索引
        IndexInfo indexInfo = findIndexForPredicate(fromTableInfo.getTableName(), ast.whereClause());

        if (indexInfo != null) {
            // 优化器：发现可以使用索引，生成 IndexScan 计划
            System.out.println("[Planner] Index found for '" + fromTableInfo.getTableName() + "." + indexInfo.getColumnName() + "'. Using Index Scan.");
            Value searchValue = extractValueFromPredicate(ast.whereClause());
            plan = new IndexScanPlanNode(fromTableInfo, indexInfo, searchValue);
        } else {
            // 优化器：没有找到合适的索引
            System.out.println("[Planner] No suitable index found for query. Using Sequential Scan.");
            // 将 WHERE 子句（如果存在）直接下推到 SeqScanPlanNode
            plan = new SeqScanPlanNode(fromTableInfo, ast.whereClause());
            // 注意：我们不再在这里创建 FilterPlanNode，因为过滤逻辑已经被下推了
        }
        // 3. JOIN 层
        if (ast.joinTable() != null) {
            TableInfo rightTableInfo = catalog.getTable(ast.joinTable().getName());
            PlanNode rightPlan = new SeqScanPlanNode(rightTableInfo, null);
            plan = new JoinPlanNode(plan, rightPlan, ast.joinCondition());
        }

        // 4. 聚合与 HAVING 过滤层
        // --- START OF FIX ---
        List<AggregateExpressionNode> selectAggregates = ast.selectList().stream()
                .filter(AggregateExpressionNode.class::isInstance)
                .map(AggregateExpressionNode.class::cast)
                .toList();

        List<AggregateExpressionNode> havingAggregates = new ArrayList<>();
        if (ast.havingClause() != null) {
            collectAggregates(ast.havingClause(), havingAggregates);
        }

        // 合并并去重所有需要计算的聚合函数
        List<AggregateExpressionNode> allAggregates = new ArrayList<>(selectAggregates);
        for (AggregateExpressionNode havingAgg : havingAggregates) {
            // 使用 toString() 作为唯一标识来判断是否已存在
            if (allAggregates.stream().noneMatch(agg -> agg.toString().equals(havingAgg.toString()))) {
                allAggregates.add(havingAgg);
            }
        }

        boolean hasGroupBy = ast.groupByClause() != null && !ast.groupByClause().isEmpty();
        if (!allAggregates.isEmpty() || hasGroupBy) {
            // 为聚合步骤构建中间 Schema，它包含所有分组列和所有聚合列
            List<Column> intermediateColumns = new ArrayList<>();
            if (ast.groupByClause() != null) {
                for (IdentifierNode groupByCol : ast.groupByClause()) {
                    intermediateColumns.add(findColumnInSchema(plan.getOutputSchema(), groupByCol.getName()));
                }
            }
            for (AggregateExpressionNode agg : allAggregates) {
                // 简化处理，所有聚合结果都为 INT
                intermediateColumns.add(new Column(agg.toString(), DataType.INT));
            }
            Schema intermediateSchema = new Schema(intermediateColumns);

            // 创建 AggregatePlanNode，传入所有聚合函数
            plan = new AggregatePlanNode(plan, ast.groupByClause(), allAggregates, intermediateSchema, ast.havingClause());
        }
        // --- END OF FIX ---

        // 5. 最终投影层
        if (!ast.isSelectAll()) {
            Schema inputForProjectionSchema = plan.getOutputSchema();
            List<Column> finalProjectedColumns = new ArrayList<>();
            for (ExpressionNode expr : ast.selectList()) {
                String colName;
                if (expr instanceof IdentifierNode idNode) {
                    colName = idNode.getName();
                } else if (expr instanceof AggregateExpressionNode aggNode) {
                    colName = aggNode.toString();
                } else {
                    // Should be caught by semantic analysis
                    continue;
                }
                finalProjectedColumns.add(findColumnInSchema(inputForProjectionSchema, colName));
            }
            Schema finalSchema = new Schema(finalProjectedColumns);
            plan = new ProjectPlanNode(plan, finalSchema);
        }

        // 6. 排序层
        if (ast.orderByClause() != null) {
            plan = new SortPlanNode(plan, ast.orderByClause());
        }

        // 7. LIMIT 层
        if (ast.limitClause() != null) {
            plan = new LimitPlanNode(plan, ast.limitClause().limit());
        }

        return plan;
    }

    /**
     * 新增的辅助方法，用于递归地从表达式树中收集所有聚合函数节点。
     */
    private void collectAggregates(ExpressionNode node, List<AggregateExpressionNode> list) {
        if (node instanceof AggregateExpressionNode aggNode) {
            list.add(aggNode);
        } else if (node instanceof BinaryExpressionNode binNode) {
            collectAggregates(binNode.left(), list);
            collectAggregates(binNode.right(), list);
        }
    }
//    /**
//     * 【最终版本】创建 SELECT 查询计划，完整集成了索引选择和您原有的 JOIN 等逻辑。
//     */
//    private PlanNode createSelectPlan(SelectStatementNode ast) {
//        String fromTableName = ast.fromTable().getName();
//        TableInfo fromTableInfo = catalog.getTable(fromTableName);
//        PlanNode plan;
//
//        IndexInfo indexInfo = findIndexForPredicate(fromTableName, ast.whereClause());
//
//        if (indexInfo != null) {
//            System.out.println("[Planner] Index found for '" + fromTableName + "." + indexInfo.getColumnName() + "'. Using Index Scan.");
//            Value searchValue = extractValueFromPredicate(ast.whereClause());
//            plan = new IndexScanPlanNode(fromTableInfo, indexInfo, searchValue);
//        } else {
//            System.out.println("[Planner] No suitable index found for query. Using Sequential Scan.");
//            plan = new SeqScanPlanNode(fromTableInfo);
//            if (ast.whereClause() != null) {
//                plan = new FilterPlanNode(plan, ast.whereClause());
//            }
//        }
//
//        if (ast.joinTable() != null) {
//            TableInfo rightTableInfo = catalog.getTable(ast.joinTable().getName());
//            PlanNode rightPlan = new SeqScanPlanNode(rightTableInfo);
//            plan = new JoinPlanNode(plan, rightPlan, ast.joinCondition());
//        }
//        // 集成 HAVING 子句
//        boolean hasAggregate = ast.selectList().stream()
//                .anyMatch(expr -> expr instanceof AggregateExpressionNode);
//        if (hasAggregate || (ast.groupByClause() != null && !ast.groupByClause().isEmpty())) {
//            List<AggregateExpressionNode> aggregates = ast.selectList().stream()
//                    .filter(AggregateExpressionNode.class::isInstance)
//                    .map(AggregateExpressionNode.class::cast)
//                    .collect(Collectors.toList());
//
//            // 构造输出 Schema
//            List<Column> outputColumns = new ArrayList<>();
//            if (ast.groupByClause() != null) {
//                for (IdentifierNode groupByCol : ast.groupByClause()) {
//                    outputColumns.add(findColumnInSchema(plan.getOutputSchema(), groupByCol.getName()));
//                }
//            }
//            for (AggregateExpressionNode agg : aggregates) {
//                // 这是一个简化。真实系统中，AVG可能是DECIMAL，COUNT是LONG等。
//                outputColumns.add(new Column(agg.toString(), DataType.INT));
//            }
//            Schema aggSchema = new Schema(outputColumns);
//            // 将 havingClause 传递给 AggregatePlanNode
//            plan = new AggregatePlanNode(plan, ast.groupByClause(), aggregates, aggSchema, ast.havingClause());
//        }
//        if (!ast.isSelectAll()&& !hasAggregate) {//若有聚合，投影已经在聚合节点中完成
//            Schema inputSchemaForProject = plan.getOutputSchema();
//
//            List<Column> projectedColumns = new ArrayList<>();
//            for (ExpressionNode expr : ast.selectList()) {
//                if (expr instanceof IdentifierNode idNode) {
//                    Column originalColumn = findColumnInSchema(inputSchemaForProject, idNode.getName());
//                    projectedColumns.add(originalColumn);
//                }
//            }
//            Schema projectedSchema = new Schema(projectedColumns);
//            plan = new ProjectPlanNode(plan, projectedSchema);
//        }
//
//        if (ast.orderByClause() != null) {
//            plan = new SortPlanNode(plan, ast.orderByClause());
//        }
//        if (ast.limitClause() != null) {
//            plan = new LimitPlanNode(plan, ast.limitClause().limit());
//        }
//
//        return plan;
//    }

    private PlanNode createDeletePlan(DeleteStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        PlanNode childPlan = new SeqScanPlanNode(tableInfo, ast.whereClause());
        if (ast.whereClause() != null) {
            childPlan = new FilterPlanNode(childPlan, ast.whereClause());
        }
        return new DeletePlanNode(childPlan, tableInfo);
    }

    private PlanNode createUpdatePlan(UpdateStatementNode ast) {
        TableInfo tableInfo = catalog.getTable(ast.tableName().getName());
        PlanNode childPlan = new SeqScanPlanNode(tableInfo, ast.whereClause());
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