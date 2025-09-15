package org.csu.sdolp.engine;

import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Value;
import org.csu.sdolp.compiler.parser.ast.expression.BinaryExpressionNode;
import org.csu.sdolp.compiler.parser.ast.ExpressionNode;
import org.csu.sdolp.compiler.parser.ast.expression.IdentifierNode;
import org.csu.sdolp.compiler.parser.ast.expression.LiteralNode;
import org.csu.sdolp.compiler.planner.plan.*;
import org.csu.sdolp.compiler.planner.plan.dcl.CreateUserPlanNode;
import org.csu.sdolp.compiler.planner.plan.dcl.GrantPlanNode;
import org.csu.sdolp.compiler.planner.plan.ddl.*;
import org.csu.sdolp.compiler.planner.plan.dml.DeletePlanNode;
import org.csu.sdolp.compiler.planner.plan.dml.InsertPlanNode;
import org.csu.sdolp.compiler.planner.plan.dml.UpdatePlanNode;
import org.csu.sdolp.compiler.planner.plan.query.*;
import org.csu.sdolp.compiler.planner.plan.show.*;
import org.csu.sdolp.executor.*;
import org.csu.sdolp.executor.dcl.CreateUserExecutor;
import org.csu.sdolp.executor.dcl.GrantExecutor;
import org.csu.sdolp.executor.ddl.*;
import org.csu.sdolp.executor.dml.*;
import org.csu.sdolp.executor.expressions.AbstractPredicate;
import org.csu.sdolp.executor.expressions.ComparisonPredicate;
import org.csu.sdolp.executor.show.*;
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
    private final DatabaseManager dbManager;


    public ExecutionEngine(BufferPoolManager bufferPoolManager, Catalog catalog,
                           LogManager logManager, LockManager lockManager, DatabaseManager dbManager) { // <-- 新增
        this.bufferPoolManager = bufferPoolManager;
        this.catalog = catalog;
        this.logManager = logManager;
        this.lockManager = lockManager;
        this.dbManager = dbManager;
    }

    public TupleIterator execute(PlanNode plan, Transaction txn) throws IOException, InterruptedException {
        return buildExecutorTree(plan, txn, LockManager.LockMode.SHARED);
    }

    private TupleIterator buildExecutorTree(PlanNode plan, Transaction txn) throws IOException, InterruptedException {
        // --- DDL/DCL Executors for Databases ---
        if (plan instanceof CreateDatabasePlanNode createDbPlan) {
            return new CreateDatabaseExecutor(createDbPlan, dbManager);
        }
        if (plan instanceof ShowDatabasesPlanNode showDbPlan) {
            return new ShowDatabasesExecutor(showDbPlan, dbManager);
        }
        if (plan instanceof ShowColumnsPlanNode showColumnsPlan) {
            return new ShowColumnsExecutor(showColumnsPlan, catalog);
        }
        if (plan instanceof ShowCreateTablePlanNode showCreateTablePlan) {
            return new ShowCreateTableExecutor(showCreateTablePlan);
        }
        if (plan instanceof DropDatabasePlanNode dropDbPlan) {
            return new DropDatabaseExecutor(dropDbPlan, dbManager);
        }

        // --- DML and Scan Executors ---
        if (plan instanceof InsertPlanNode insertPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, insertPlan.getTableInfo(), logManager, lockManager);
            return new InsertExecutor(insertPlan, tableHeap, txn, catalog, bufferPoolManager);
        }
        if (plan instanceof SeqScanPlanNode seqScanPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, seqScanPlan.getTableInfo(), logManager, lockManager);
            return new SeqScanExecutor(tableHeap, txn);
        }
        if (plan instanceof DeletePlanNode deletePlan) {
            PlanNode childPlanForDelete = new SeqScanPlanNode(deletePlan.getTableInfo());
            if (deletePlan.getChild() instanceof FilterPlanNode) {
                childPlanForDelete = new FilterPlanNode(childPlanForDelete, ((FilterPlanNode) deletePlan.getChild()).getPredicate());
            }
            TupleIterator childExecutor = buildExecutorTree(childPlanForDelete, txn, LockManager.LockMode.EXCLUSIVE);
            TableHeap tableHeap = new TableHeap(bufferPoolManager, deletePlan.getTableInfo(), logManager, lockManager);
            return new DeleteExecutor(deletePlan, childExecutor, tableHeap, txn, catalog, bufferPoolManager);
        }
        if (plan instanceof UpdatePlanNode updatePlan) {
            // 为 UPDATE 操作的子计划（扫描器）明确指定排他锁
            PlanNode childPlanForUpdate = new SeqScanPlanNode(updatePlan.getTableInfo());
            if (updatePlan.getChild() instanceof FilterPlanNode) {
                childPlanForUpdate = new FilterPlanNode(childPlanForUpdate, ((FilterPlanNode) updatePlan.getChild()).getPredicate());
            }
            TupleIterator childExecutor = buildExecutorTree(childPlanForUpdate, txn, LockManager.LockMode.EXCLUSIVE);

            TableHeap tableHeap = new TableHeap(bufferPoolManager, updatePlan.getTableInfo(), logManager, lockManager);
            return new UpdateExecutor(
                    childExecutor,
                    tableHeap,
                    updatePlan.getTableInfo().getSchema(),
                    updatePlan.getSetClauses(),
                    txn,
                    catalog,
                    bufferPoolManager
            );
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
        if (plan instanceof CreateIndexPlanNode createIndexPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, createIndexPlan.getTableInfo(), logManager, lockManager);
            return new CreateIndexExecutor(createIndexPlan, tableHeap, catalog, bufferPoolManager, txn);
        }

        if (plan instanceof IndexScanPlanNode indexScanPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, indexScanPlan.getTableInfo(), logManager, lockManager);
            return new IndexScanExecutor(indexScanPlan, tableHeap, bufferPoolManager, txn);
        }
        //show修改
        if (plan instanceof ShowTablesPlanNode showTablesPlan) {
            return new ShowTablesExecutor(showTablesPlan, catalog);
        }
        // --- DDL Executors ---
        if (plan instanceof CreateTablePlanNode createTablePlan) {
            return new CreateTableExecutor(createTablePlan, catalog, txn, logManager, bufferPoolManager, lockManager);
        }
        if (plan instanceof DropTablePlanNode dropPlan) {
            return new DropTableExecutor(dropPlan, catalog, txn, logManager);
        }
        if (plan instanceof AlterTablePlanNode alterPlan) {
            return new AlterTableExecutor(alterPlan, catalog, txn, logManager);
        }
        // ======  (Phase 4) ======
        if (plan instanceof AggregatePlanNode aggPlan) {
            TupleIterator childExecutor = buildExecutorTree(aggPlan.getChild(), txn);
            return new AggregateExecutor(childExecutor, aggPlan);
        }
        if (plan instanceof CreateUserPlanNode createUserPlan) {
            return new CreateUserExecutor(createUserPlan, catalog, txn);
        }
        if (plan instanceof GrantPlanNode grantPlan) {
            return new GrantExecutor(grantPlan, catalog, txn);
        }
        throw new UnsupportedOperationException("Unsupported plan node: " + plan.getClass().getSimpleName());
    }
    private TupleIterator buildExecutorTree(PlanNode plan, Transaction txn, LockManager.LockMode lockMode) throws IOException, InterruptedException {
        if (plan instanceof SeqScanPlanNode seqScanPlan) {
            TableHeap tableHeap = new TableHeap(bufferPoolManager, seqScanPlan.getTableInfo(), logManager, lockManager);
            return new SeqScanExecutor(tableHeap, txn, lockMode);
        }
        if (plan instanceof FilterPlanNode filterPlan) {
            // 将锁模式向下传递
            TupleIterator childExecutor = buildExecutorTree(filterPlan.getChild(), txn, lockMode);
            AbstractPredicate predicate = createPredicateFromAst(filterPlan.getPredicate(), filterPlan.getChild().getOutputSchema());
            return new FilterExecutor(childExecutor, predicate);
        }
        // 如果是其他类型的节点，则使用默认的构建方法
        return buildExecutorTree(plan, txn);
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