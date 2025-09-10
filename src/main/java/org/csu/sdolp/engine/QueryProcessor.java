package org.csu.sdolp.engine;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.common.exception.ParseException;
import org.csu.sdolp.common.exception.SemanticException;
import org.csu.sdolp.common.model.Schema;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    /**
     * 主执行方法，它会捕获所有异常并将其格式化为错误信息返回。
     * 它将执行结果或错误信息作为字符串返回。
     *
     * @param sql 要执行的SQL语句
     * @return 包含结果集或错误信息的字符串
     */
    public String executeAndGetResult(String sql) {
        Transaction txn = null; // 提前声明
        try {
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
                transactionManager.abort(txn); // 如果是空语句，也中止事务
                return "Empty statement.";
            }

            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast);

            PlanNode plan = planner.createPlan(ast);

            // *** 修改点：将Transaction对象传入execute方法 ***
            TupleIterator executor = executionEngine.execute(plan, txn);


            String result = formatResults(executor); // 调用新的格式化方法

            transactionManager.commit(txn);
            return result;
        } catch (Exception e) {
            if (txn != null) {
                try {
                    System.err.println("Error occurred, aborting transaction " + txn.getTransactionId());
                    transactionManager.abort(txn);
                } catch (IOException ioException) {
                    System.err.println("Failed to abort transaction: " + ioException.getMessage());
                }
            }
            // 将异常信息格式化后返回
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return "ERROR: " + e.getMessage(); // 返回简洁的错误信息
        }
    }

    public void execute(String sql) {
        String result = executeAndGetResult(sql);
        System.out.println(result);
    }

    /**
     * 将执行器返回的元组迭代器格式化为字符串。
     *
     * @param iterator 元组迭代器
     * @return 格式化后的结果字符串
     */
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

        if (results.isEmpty()) {
            if (schema != null && !schema.getColumns().isEmpty()) {
                // 如果是 SELECT 语句但没有结果，打印表头和空集信息
                StringBuilder sb = new StringBuilder();
                List<String> columnNames = schema.getColumnNames();
                List<Integer> columnWidths = columnNames.stream().map(String::length).collect(Collectors.toList());

                sb.append(getSeparator(columnWidths)).append("\n");
                sb.append(getRow(columnNames, columnWidths)).append("\n");
                sb.append(getSeparator(columnWidths)).append("\n");
                sb.append("Query finished, 0 rows returned.");
                return sb.toString();
            }
            // 对应 UPDATE/DELETE 影响0行的情况
            return "Query OK, 0 rows affected.";
        }

        // --- 核心打印逻辑 ---
        StringBuilder sb = new StringBuilder();
        List<String> columnNames = schema.getColumnNames();

        // 1. 计算每列的最大宽度
        List<Integer> columnWidths = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            int maxWidth = columnNames.get(i).length();
            for (Tuple tuple : results) {
                maxWidth = Math.max(maxWidth, tuple.getValues().get(i).getValue().toString().length());
            }
            columnWidths.add(maxWidth);
        }

        // 2. 打印边框和表头
        sb.append(getSeparator(columnWidths)).append("\n");
        sb.append(getRow(columnNames, columnWidths)).append("\n");
        sb.append(getSeparator(columnWidths)).append("\n");

        // 3. 打印数据行
        for (Tuple tuple : results) {
            List<String> values = tuple.getValues().stream()
                    .map(v -> v.getValue().toString())
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
}