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

/**
 * 数据库查询处理器核心引擎。
 * 负责接收SQL字符串，并协调编译器、执行器和存储层来完成整个查询处理流程。
 * 这是连接所有数据库组件的中央枢纽。
 */
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
            this.executionEngine = new ExecutionEngine(bufferPoolManager, catalog);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database engine", e);
        }
    }

    public void close() throws IOException{
        // 确保所有数据在关闭前都写入磁盘
        bufferPoolManager.flushAllPages();
        logManager.flush(); // 关闭前也确保日志刷盘
        diskManager.close();
        logManager.close();
    }

    /**
     * 执行一条SQL语句的主入口方法。
     * @param sql 用户输入的SQL字符串
     */
    public void execute(String sql) {
        Transaction txn = transactionManager.begin();
        try {
            System.out.println("Executing: " + sql);

            // 1. 词法分析: String -> List<Token>
            Lexer lexer = new Lexer(sql);
            List<Token> tokens = lexer.tokenize();

            // 2. 语法分析: List<Token> -> Abstract Syntax Tree (AST)
            Parser parser = new Parser(tokens);
            StatementNode ast = parser.parse();
            if (ast == null) return;

            // 3. 语义分析: 检查AST的逻辑正确性
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);
            System.out.println("Semantic analysis passed.");

            // 4. 计划生成: AST -> PlanNode Tree
            PlanNode plan = planner.createPlan(ast);
            System.out.println("Logical plan created.");

            // 5. 执行: PlanNode Tree -> TupleIterator Tree -> Results
            TupleIterator executor = executionEngine.execute(plan);
            System.out.println("Execution plan executed.");

            // 暂时简化，我们在执行后直接提交
            transactionManager.commit(txn);

            // 6. 打印结果
            if (executor != null) {
                printResults(executor);
            }

        } catch (ParseException | SemanticException | UnsupportedOperationException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            // ---- 发生任何错误时，中止事务 ----
            System.err.println("Error occurred, aborting transaction " + txn.getTransactionId());
            transactionManager.abort(txn);

            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
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