package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.BinaryExpressionNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.LiteralNode;
import org.csu.sdolp.compiler.planner.plan.*;
import org.csu.sdolp.executor.*;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import org.csu.sdolp.executor.expressions.ComparisonPredicate;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.log.LogManager;
import org.csu.sdolp.executor.expressions.LogicalPredicate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class ExecutionEngine {
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;
    private final LogManager logManager;

    public ExecutionEngine(BufferPoolManager bufferPoolManager, Catalog catalog, LogManager logManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
        this.logManager = logManager;
    }

    public TupleIterator execute(PlanNode plan, Transaction txn) throws IOException {
        return buildExecutorTree(plan, txn);
    }

    private TupleIterator buildExecutorTree(PlanNode plan, Transaction txn) throws IOException {
        if (plan instanceof CreateTablePlanNode) {
            return new CreateTableExecutor((CreateTablePlanNode) plan, catalog);
        }
        if (plan instanceof InsertPlanNode insertPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, insertPlan.getTableInfo(), logManager);
            return new InsertExecutor(insertPlan, tableHeap, txn);
        }
        if (plan instanceof SeqScanPlanNode seqScanPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, seqScanPlan.getTableInfo(), logManager);
            return new SeqScanExecutor(tableHeap);
        }

        // --- 修正点 1 ---
        // 将 plan 转换为 FilterPlanNode 类型的变量 filterPlan
        if (plan instanceof FilterPlanNode filterPlan) {
            TupleIterator childExecutor = buildExecutorTree(filterPlan.getChild(), txn);
            AbstractPredicate predicate = createPredicateFromAst(filterPlan.getPredicate(), filterPlan.getChild().getOutputSchema());
            return new FilterExecutor(childExecutor, predicate);
        }

        // --- 修正点 2 ---
        // 将 plan 转换为 ProjectPlanNode 类型的变量 projectPlan
        if (plan instanceof ProjectPlanNode projectPlan) {
            TupleIterator childExecutor = buildExecutorTree(projectPlan.getChild(), txn);
            List<Integer> columnIndexes = new ArrayList<>();
            Schema childSchema = projectPlan.getChild().getOutputSchema();
            for(String colName : projectPlan.getOutputSchema().getColumns().stream().map(c -> c.getName()).collect(Collectors.toList())) {
                for(int i = 0; i < childSchema.getColumns().size(); i++) {
                    if(childSchema.getColumns().get(i).getName().equalsIgnoreCase(colName)) {
                        columnIndexes.add(i);
                        break;
                    }
                }
            }
            return new ProjectExecutor(childExecutor, columnIndexes);
        }

        // --- 修正点 3 ---
        // 将 plan 转换为 DeletePlanNode 类型的变量 deletePlan
        if (plan instanceof DeletePlanNode deletePlan) {
            TupleIterator childPlan = buildExecutorTree(deletePlan.getChild(), txn);
            TableHeap tableHeap = new TableHeap(bufferPoolManager, deletePlan.getTableInfo(), logManager);
            return new DeleteExecutor(childPlan, tableHeap, txn);
        }

        // --- 修正点 4 ---
        // 将 plan 转换为 UpdatePlanNode 类型的变量 updatePlan
        if (plan instanceof UpdatePlanNode updatePlan) {
            TupleIterator childPlan = buildExecutorTree(updatePlan.getChild(), txn);
            TableHeap tableHeap = new TableHeap(bufferPoolManager, updatePlan.getTableInfo(), logManager);
            return new UpdateExecutor(childPlan, tableHeap, updatePlan.getTableInfo().getSchema(), updatePlan.getSetClauses(), txn);
        }
        if (plan instanceof SortPlanNode sortPlan) {
            // 错误修正：在递归调用时，必须传递 txn 对象
            TupleIterator childExecutor = buildExecutorTree(sortPlan.getChild(), txn);
            return new SortExecutor(childExecutor, sortPlan);
        }
        if (plan instanceof LimitPlanNode limitPlan) {
            // 错误修正：在递归调用时，必须传递 txn 对象
            TupleIterator childExecutor = buildExecutorTree(limitPlan.getChild(), txn);
            return new LimitExecutor(childExecutor, limitPlan.getLimit());
        }
        if (plan instanceof DropTablePlanNode dropPlan) {
            return new DropTableExecutor(dropPlan, catalog);
        }
        if (plan instanceof AlterTablePlanNode alterPlan) {
            return new AlterTableExecutor(alterPlan, catalog);
        }
        throw new UnsupportedOperationException("Unsupported plan node: " + plan.getClass().getSimpleName());
    }

    private AbstractPredicate createPredicateFromAst(ExpressionNode expression, Schema schema) {
        if (expression instanceof BinaryExpressionNode node) {
            String operatorName = node.operator().type().name();
            // 检查是否是逻辑运算符
            if ("AND".equals(operatorName) || "OR".equals(operatorName)) {
                AbstractPredicate left = createPredicateFromAst(node.left(), schema);
                AbstractPredicate right = createPredicateFromAst(node.right(), schema);
                return new LogicalPredicate(left, right, operatorName);
            }

            // 否则，是比较运算符
            if (!(node.left() instanceof IdentifierNode) || !(node.right() instanceof LiteralNode)) {
                throw new UnsupportedOperationException("WHERE clause only supports 'column_name op literal' format.");
            }

            String columnName = ((IdentifierNode) node.left()).name();
            int columnIndex = getColumnIndex(schema, columnName);
            Value literalValue = getLiteralValue((LiteralNode) node.right());

            return new ComparisonPredicate(columnIndex, literalValue, operatorName);
        }
        throw new UnsupportedOperationException("Unsupported expression type in WHERE clause: " + expression.getClass().getSimpleName());
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