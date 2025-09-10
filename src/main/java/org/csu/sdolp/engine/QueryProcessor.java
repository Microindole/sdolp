package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.lexer.Token;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.csu.sdolp.compiler.planner.Planner;
import org.csu.sdolp.compiler.planner.plan.PlanNode;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.Transaction;
import org.csu.sdolp.transaction.TransactionManager;
import org.csu.sdolp.transaction.log.LogManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class QueryProcessor {

    private final DiskManager diskManager;
    private final BufferPoolManager bufferPoolManager;
    private final Catalog catalog;
    private final Planner planner;
    private final ExecutionEngine executionEngine;
    private final LogManager logManager;
    private final LockManager lockManager;
    private final TransactionManager transactionManager;

    public QueryProcessor(String dbFilePath) {
        try {
            this.diskManager = new DiskManager(dbFilePath);
            diskManager.open();
            final int bufferPoolSize = 100;
            this.bufferPoolManager = new BufferPoolManager(bufferPoolSize, diskManager, "LRU");
            this.catalog = new Catalog(bufferPoolManager);
            this.planner = new Planner(catalog);
            this.logManager = new LogManager(dbFilePath + ".log");
            this.lockManager = new LockManager();
            this.transactionManager = new TransactionManager(lockManager, logManager);
            // *** 修改点：将LogManager传入ExecutionEngine ***
            this.executionEngine = new ExecutionEngine(bufferPoolManager, catalog, logManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database engine", e);
        }
    }

    public void close() throws IOException {
        bufferPoolManager.flushAllPages();
        logManager.flush();
        diskManager.close();
        logManager.close();
    }

    public void execute(String sql) {
        Transaction txn = null; // 提前声明
        try {
            txn = transactionManager.begin();
            System.out.println("Executing: " + sql);

            Lexer lexer = new Lexer(sql);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens);
            StatementNode ast = parser.parse();
            if (ast == null) {
                transactionManager.abort(txn); // 如果是空语句，也中止事务
                return;
            }

            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);
            System.out.println("Semantic analysis passed.");

            PlanNode plan = planner.createPlan(ast);
            System.out.println("Logical plan created.");

            // *** 修改点：将Transaction对象传入execute方法 ***
            TupleIterator executor = executionEngine.execute(plan, txn);
            System.out.println("Execution plan executed.");

            // 执行器可能为null（如CREATE），或者迭代完成后才算成功
            if (executor != null) {
                printResults(executor);
            }

            transactionManager.commit(txn);

        } catch (Exception e) {
            if (txn != null) {
                try {
                    System.err.println("Error occurred, aborting transaction " + txn.getTransactionId());
                    transactionManager.abort(txn);
                } catch (IOException ioException) {
                    System.err.println("Failed to abort transaction: " + ioException.getMessage());
                }
            }
            System.err.println("Error: " + e.getMessage());
            if (!(e instanceof ParseException || e instanceof SemanticException)) {
                e.printStackTrace();
            }
        }
    }

    private void printResults(TupleIterator iterator) throws IOException {
        List<Tuple> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        if (results.isEmpty()) {
            System.out.println("Query finished, 0 rows affected or returned.");
            return;
        }

        for (Tuple tuple : results) {
            System.out.println(">> " + tuple);
        }
        System.out.println("Query finished, " + results.size() + " rows returned/affected.");
    }
}