package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.catalog.TableInfo;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.*;
import org.csu.sdolp.compiler.planner.plan.*;
import org.csu.sdolp.executor.*;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import org.csu.sdolp.executor.expressions.ComparisonPredicate;
import org.csu.sdolp.storage.buffer.BufferPoolManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 执行引擎.
 * 负责将逻辑执行计划(PlanNode)转换为物理执行计划(TupleIterator)树并执行它.
 */
public class ExecutionEngine {
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;

    public ExecutionEngine(BufferPoolManager bufferPoolManager, Catalog catalog) {
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
    }

    /**
     * 执行一个执行计划
     * @param plan 逻辑计划树的根节点
     * @return 如果是查询语句，则返回结果的迭代器；如果是DDL/DML，则返回null
     * @throws IOException
     */
    public TupleIterator execute(PlanNode plan) throws IOException {
        return buildExecutorTree(plan);
    }

    /**
     * 递归地将PlanNode树转换为Executor树
     * @param plan 计划节点
     * @return 执行器节点
     * @throws IOException
     */
    private TupleIterator buildExecutorTree(PlanNode plan) throws IOException {
        if (plan instanceof CreateTablePlanNode) {
            return new CreateTableExecutor((CreateTablePlanNode) plan, catalog);
        }
        if (plan instanceof InsertPlanNode) {
            InsertPlanNode insertPlan = (InsertPlanNode) plan;
            TableHeap tableHeap = new TableHeap(bufferPoolManager, insertPlan.getTableInfo());
            return new InsertExecutor(insertPlan, tableHeap);
        }
        if (plan instanceof SeqScanPlanNode) {
            SeqScanPlanNode seqScanPlan = (SeqScanPlanNode) plan;
            return new SeqScanExecutor(bufferPoolManager, seqScanPlan.getTableInfo());
        }
        if (plan instanceof FilterPlanNode) {
            FilterPlanNode filterPlan = (FilterPlanNode) plan;
            TupleIterator childExecutor = buildExecutorTree(filterPlan.getChild());
            AbstractPredicate predicate = createPredicateFromAst(filterPlan.getPredicate(), filterPlan.getChild().getOutputSchema());
            return new FilterExecutor(childExecutor, predicate);
        }
        if (plan instanceof ProjectPlanNode) {
            ProjectPlanNode projectPlan = (ProjectPlanNode) plan;
            TupleIterator childExecutor = buildExecutorTree(projectPlan.getChild());
            
            // 计算需要投影的列在子执行器输出中的索引
            List<Integer> columnIndexes = new ArrayList<>();
            Schema childSchema = projectPlan.getChild().getOutputSchema();
            Schema projectedSchema = projectPlan.getOutputSchema();
            
            for(String projectedColName : projectedSchema.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList())) {
                 for(int i=0; i<childSchema.getColumns().size(); i++){
                     if(childSchema.getColumns().get(i).getName().equalsIgnoreCase(projectedColName)){
                         columnIndexes.add(i);
                         break;
                     }
                 }
            }
            return new ProjectExecutor(childExecutor, columnIndexes);
        }
         if (plan instanceof DeletePlanNode) {
            DeletePlanNode deletePlan = (DeletePlanNode) plan;
            TupleIterator childPlan = buildExecutorTree(deletePlan.getChild());
            TableHeap tableHeap = new TableHeap(bufferPoolManager, deletePlan.getTableInfo());
            return new DeleteExecutor(childPlan, tableHeap);
        }

        if (plan instanceof UpdatePlanNode) {
            UpdatePlanNode updatePlan = (UpdatePlanNode) plan;
            TupleIterator childPlan = buildExecutorTree(updatePlan.getChild());
            TableHeap tableHeap = new TableHeap(bufferPoolManager, updatePlan.getTableInfo());
            return new UpdateExecutor(childPlan, tableHeap, updatePlan.getTableInfo().getSchema(), updatePlan.getSetClauses());
        }


        throw new UnsupportedOperationException("Unsupported plan node: " + plan.getClass().getSimpleName());
    }

    // --- 辅助方法 (从QueryProcessor迁移并改进) ---
    private AbstractPredicate createPredicateFromAst(ExpressionNode expression, Schema schema) {
        if (expression instanceof BinaryExpressionNode node) {
            if (!(node.left() instanceof IdentifierNode) || !(node.right() instanceof LiteralNode)) {
                throw new UnsupportedOperationException("WHERE clause only supports 'column_name op literal' format.");
            }

            String columnName = ((IdentifierNode) node.left()).name();
            int columnIndex = getColumnIndex(schema, columnName);
            String operator = node.operator().type().name();
            Value literalValue = getLiteralValue((LiteralNode) node.right());

            return new ComparisonPredicate(columnIndex, literalValue, operator);
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE clause.");
    }

    private int getColumnIndex(Schema schema, String columnName) {
        for (int i = 0; i < schema.getColumns().size(); i++) {
            if (schema.getColumns().get(i).getName().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new IllegalStateException("Column '" + columnName + "' not found in schema during execution planning.");
    }

    private Value getLiteralValue(LiteralNode literalNode) {
        String lexeme = literalNode.literal().lexeme();
        return switch (literalNode.literal().type()) {
            case INTEGER_CONST -> new Value(Integer.parseInt(lexeme));
            case STRING_CONST -> new Value(lexeme);
            default -> throw new IllegalStateException("Unsupported literal type in expression.");
        };
    }
}