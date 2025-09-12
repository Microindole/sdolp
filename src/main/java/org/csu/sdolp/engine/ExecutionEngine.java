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
import org.csu.sdolp.transaction.LockManager;
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
    private final LockManager lockManager;


    public ExecutionEngine(BufferPoolManager bufferPoolManager, Catalog catalog,
                           LogManager logManager, LockManager lockManager) {
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
        this.logManager = logManager;
        this.lockManager = lockManager;
    }

    public TupleIterator execute(PlanNode plan, Transaction txn) throws IOException, InterruptedException {
        return buildExecutorTree(plan, txn);
    }

    private TupleIterator buildExecutorTree(PlanNode plan, Transaction txn) throws IOException, InterruptedException {
        // --- DML and Scan Executors ---
        if (plan instanceof InsertPlanNode insertPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, insertPlan.getTableInfo(), logManager, lockManager);
            return new InsertExecutor(insertPlan, tableHeap, txn);
        }
        if (plan instanceof SeqScanPlanNode seqScanPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, seqScanPlan.getTableInfo(), logManager, lockManager);
            return new SeqScanExecutor(tableHeap, txn);
        }
        if (plan instanceof DeletePlanNode deletePlan) {
            TupleIterator childPlan = buildExecutorTree(deletePlan.getChild(), txn);
            TableHeap tableHeap = new TableHeap(bufferPoolManager, deletePlan.getTableInfo(), logManager, lockManager);
            return new DeleteExecutor(childPlan, tableHeap, txn);
        }
        if (plan instanceof UpdatePlanNode updatePlan) {
            TupleIterator childPlan = buildExecutorTree(updatePlan.getChild(), txn);
            TableHeap tableHeap = new TableHeap(bufferPoolManager, updatePlan.getTableInfo(), logManager, lockManager);
            return new UpdateExecutor(childPlan, tableHeap, updatePlan.getTableInfo().getSchema(), updatePlan.getSetClauses(), txn);
        }

        // --- Query Clause Executors ---
        if (plan instanceof FilterPlanNode filterPlan) {
            TupleIterator childExecutor = buildExecutorTree(filterPlan.getChild(), txn);
            AbstractPredicate predicate = createPredicateFromAst(filterPlan.getPredicate(), filterPlan.getChild().getOutputSchema());
            return new FilterExecutor(childExecutor, predicate);
        }
        if (plan instanceof ProjectPlanNode projectPlan) {
            TupleIterator childExecutor = buildExecutorTree(projectPlan.getChild(), txn);
            List<Integer> columnIndexes = new ArrayList<>();
            Schema childSchema = projectPlan.getChild().getOutputSchema();
            for (String colName : projectPlan.getOutputSchema().getColumns().stream().map(c -> c.getName()).collect(Collectors.toList())) {
                for (int i = 0; i < childSchema.getColumns().size(); i++) {
                    if (childSchema.getColumns().get(i).getName().equalsIgnoreCase(colName)) {
                        columnIndexes.add(i);
                        break;
                    }
                }
            }
            return new ProjectExecutor(childExecutor, columnIndexes);
        }
        if (plan instanceof SortPlanNode sortPlan) {
            TupleIterator childExecutor = buildExecutorTree(sortPlan.getChild(), txn);
            return new SortExecutor(childExecutor, sortPlan);
        }
        if (plan instanceof LimitPlanNode limitPlan) {
            TupleIterator childExecutor = buildExecutorTree(limitPlan.getChild(), txn);
            return new LimitExecutor(childExecutor, limitPlan.getLimit());
        }
        if (plan instanceof JoinPlanNode joinPlan) {
            TupleIterator leftExecutor = buildExecutorTree(joinPlan.getLeft(), txn);
            TupleIterator rightExecutor = buildExecutorTree(joinPlan.getRight(), txn);
            return new JoinExecutor(joinPlan, leftExecutor, rightExecutor);
        }

        // --- DDL Executors ---
        if (plan instanceof CreateTablePlanNode createTablePlan) {
            // *** 核心修复点 1：为 DDL 执行器传入 txn 和 logManager ***
            return new CreateTableExecutor(createTablePlan, catalog, txn, logManager);
        }
        if (plan instanceof DropTablePlanNode dropPlan) {
            // *** 核心修复点 2：为 DDL 执行器传入 txn 和 logManager ***
            return new DropTableExecutor(dropPlan, catalog, txn, logManager);
        }
        if (plan instanceof AlterTablePlanNode alterPlan) {
            // *** 核心修复点 3：为 DDL 执行器传入 txn 和 logManager ***
            return new AlterTableExecutor(alterPlan, catalog, txn, logManager);
        }
        // ======  (Phase 4) ======
        if (plan instanceof AggregatePlanNode aggPlan) {
            TupleIterator childExecutor = buildExecutorTree(aggPlan.getChild(), txn);
            return new AggregateExecutor(childExecutor, aggPlan);
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

            String columnName = ((IdentifierNode) node.left()).getName();
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