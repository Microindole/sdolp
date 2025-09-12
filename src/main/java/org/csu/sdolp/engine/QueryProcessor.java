package org.csu.sdolp.engine;

import lombok.Getter;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.lexer.Lexer;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryProcessor {

    private final DiskManager diskManager;
    @Getter
    private final BufferPoolManager bufferPoolManager;
    @Getter
    private final Catalog catalog;
    private final Planner planner;
    private final ExecutionEngine executionEngine;
    @Getter
    private final LogManager logManager;
    @Getter
    private final LockManager lockManager;
    @Getter
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
            this.executionEngine = new ExecutionEngine(bufferPoolManager, catalog, logManager, lockManager);
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

    public String executeAndGetResult(String sql) {
        Transaction txn = null;
        try {
            if (sql.trim().equalsIgnoreCase("CRASH_NOW;")) {
                System.out.println("[DEBUG] Received CRASH_NOW command. Simulating unexpected shutdown...");
                System.exit(1);
            }
            if (sql.trim().equalsIgnoreCase("FLUSH_BUFFER;")) {
                bufferPoolManager.clear();
                return "Buffer pool cleared.";
            }

            txn = transactionManager.begin();
            System.out.println("Executing: " + sql + " in TxnID=" + txn.getTransactionId());

            Lexer lexer = new Lexer(sql);
            Parser parser = new Parser(lexer.tokenize());
            StatementNode ast = parser.parse();
            if (ast == null) {
                transactionManager.abort(txn);
                return "Empty statement.";
            }

            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);

            PlanNode plan = planner.createPlan(ast);
            TupleIterator executor = executionEngine.execute(plan, txn);

            String result = formatResults(executor);
            transactionManager.commit(txn);
            return result;
        } catch (Exception e) {
            // 只有当事务仍处于活动状态时才中止
            if (txn != null && txn.getState() == Transaction.State.ACTIVE) {
                try {
                    System.err.println("Error occurred, aborting transaction " + txn.getTransactionId());
                    transactionManager.abort(txn);
                } catch (IOException ioException) {
                    System.err.println("Failed to abort transaction: " + ioException.getMessage());
                }
            }
            // 打印堆栈信息以帮助调试
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.err.println("Error details: " + sw.toString()); // 在服务端打印详细错误
            return "ERROR: " + e.getMessage(); // 给客户端返回简洁错误
        }
    }

    private String formatResults(TupleIterator iterator) throws IOException {
        if (iterator == null) {
            return "Query OK.";
        }
        List<Tuple> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        if (results.isEmpty()) {
            return "Query finished, 0 rows affected or returned.";
        }

        Schema schema = iterator.getOutputSchema();
        StringBuilder sb = new StringBuilder();
        List<String> columnNames = schema.getColumnNames();
        List<Integer> columnWidths = new ArrayList<>();

        // 1. 计算每列的最大宽度 (null-safe)
        for (int i = 0; i < columnNames.size(); i++) {
            int maxWidth = columnNames.get(i).length();
            for (Tuple tuple : results) {
                Object value = tuple.getValues().get(i).getValue();
                String cellValue = (value == null) ? "NULL" : value.toString();
                maxWidth = Math.max(maxWidth, cellValue.length());
            }
            columnWidths.add(maxWidth);
        }

        // 2. 打印边框和表头
        sb.append(getSeparator(columnWidths)).append("\n");
        sb.append(getRow(columnNames, columnWidths)).append("\n");
        sb.append(getSeparator(columnWidths)).append("\n");

        // 3. 打印数据行 (null-safe)
        for (Tuple tuple : results) {
            List<String> values = tuple.getValues().stream()
                    // --- 这就是最终的修复！ ---
                    .map(v -> (v.getValue() == null) ? "NULL" : v.getValue().toString())
                    .collect(Collectors.toList());
            sb.append(getRow(values, columnWidths)).append("\n");
        }

        // 4. 打印底部边框和最终消息
        sb.append(getSeparator(columnWidths)).append("\n");
        sb.append("Query finished, ").append(results.size()).append(" rows returned.");

        return sb.toString();
    }


    private String getRow(List<String> cells, List<Integer> widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < cells.size(); i++) {
            sb.append(String.format(" %-" + widths.get(i) + "s |", cells.get(i)));
        }
        return sb.toString();
    }

    private String getSeparator(List<Integer> widths) {
        StringBuilder sb = new StringBuilder("+");
        for (Integer width : widths) {
            sb.append("-".repeat(width + 2)).append("+");
        }
        return sb.toString();
    }

    // 在 QueryProcessor.java 中添加这个新的 public 方法
    public TupleIterator executeMysql(String sql) throws Exception {
        Transaction txn = transactionManager.begin();
        try {
            // 处理SQL语句，自动添加分号
            String normalizedSql = normalizeSqlForMysql(sql);

            Lexer lexer = new Lexer(normalizedSql);
            Parser parser = new Parser(lexer.tokenize());
            StatementNode ast = parser.parse();
            if (ast == null) {
                transactionManager.abort(txn);
                return null;
            }
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);
            PlanNode plan = planner.createPlan(ast);
            TupleIterator iterator = executionEngine.execute(plan, txn);
            transactionManager.commit(txn);
            return iterator;
        } catch (Exception e) {
            transactionManager.abort(txn);
            throw e; // 将异常向上抛出，由 Protocol Handler 处理
        }
    }

    // 添加这个辅助方法来规范化SQL语句
    private String normalizeSqlForMysql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        String trimmedSql = sql.trim();

        // 如果SQL语句不以分号结尾，自动添加分号
        if (!trimmedSql.endsWith(";")) {
            trimmedSql += ";";
        }

        return trimmedSql;
    }

    /**
     * 为给定的SQL创建一个执行器迭代器。
     * 这个方法假定一个事务已经由调用者开启。
     * @param sql SQL 语句
     * @param txn 当前的事务
     * @return 一个可以遍历结果的元组迭代器
     */
    public TupleIterator createExecutorForQuery(String sql, Transaction txn) throws Exception {
        String normalizedSql = normalizeSqlForMysql(sql);

        Lexer lexer = new Lexer(normalizedSql);
        Parser parser = new Parser(lexer.tokenize());
        StatementNode ast = parser.parse();
        if (ast == null) {
            return null;
        }
        SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
        semanticAnalyzer.analyze(ast);
        PlanNode plan = planner.createPlan(ast);
        return executionEngine.execute(plan, txn);
    }

    public void execute(String sql){
        String result = executeAndGetResult(sql);
        System.out.println(result);
    }
}