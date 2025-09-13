package org.csu.sdolp.engine;

import lombok.Getter;
import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.cli.Session;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.CreateDatabaseStatementNode;
import org.csu.sdolp.compiler.parser.ast.ShowDatabasesStatementNode;
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

    private DiskManager diskManager;
    @Getter
    private BufferPoolManager bufferPoolManager;
    @Getter
    private Catalog catalog;
    private Planner planner;
    private ExecutionEngine executionEngine;
    @Getter
    private LogManager logManager;
    @Getter
    private LockManager lockManager;
    @Getter
    private TransactionManager transactionManager;
    @Getter // <-- 新增 Getter
    private final DatabaseManager dbManager;

    public QueryProcessor(String dbName) {
        try {
            this.dbManager = new DatabaseManager();
            this.diskManager = new DiskManager(DatabaseManager.getDbFilePath(dbName));
            diskManager.open();
            final int bufferPoolSize = 100;
            this.bufferPoolManager = new BufferPoolManager(bufferPoolSize, diskManager, "LRU");
            this.catalog = new Catalog(bufferPoolManager);
            this.planner = new Planner(catalog);
            this.logManager = new LogManager(DatabaseManager.getDbFilePath(dbName) + ".log");
            this.lockManager = new LockManager();
            this.transactionManager = new TransactionManager(lockManager, logManager);
            this.executionEngine = new ExecutionEngine(bufferPoolManager, catalog, logManager, lockManager, dbManager);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database engine for " + dbName, e);
        }
    }

    public void close() throws IOException {
        bufferPoolManager.flushAllPages();
        logManager.flush();
        diskManager.close();
        logManager.close();
    }

    public String executeAndGetResult(String sql, Session session) {
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

            Lexer lexer = new Lexer(sql);
            Parser parser = new Parser(lexer.tokenize());
            StatementNode ast = parser.parse();
            if (ast == null) {
                return "Empty statement.";
            }

            // Database-level commands are handled outside of transactions for simplicity
            if (ast instanceof CreateDatabaseStatementNode || ast instanceof ShowDatabasesStatementNode) {
                PlanNode plan = planner.createPlan(ast);
                TupleIterator executor = executionEngine.execute(plan, null);
                return formatResults(executor);
            }

            txn = transactionManager.begin();
            System.out.println("Executing: " + sql + " in TxnID=" + txn.getTransactionId());

            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast,session);

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

    public String executeAndGetResult(String sql) {
        // 这个版本现在只用于内部测试，或者假设一个已经认证的root session
        return executeAndGetResult(sql, Session.createAuthenticatedSession(-1, "root"));
    }

    private String formatResults(TupleIterator iterator) throws IOException {
        if (iterator == null) {
            return "Query OK.";
        }
        List<Tuple> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }

        Schema schema = iterator.getOutputSchema();
        if (schema == null) {
            if (results.size() == 1) {
                return results.get(0).getValues().get(0).toString();
            }
            return "Query OK.";
        }


        if (results.isEmpty()) {
            return "Query finished, 0 rows affected or returned.";
        }

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

    public TupleIterator executeMysql(String sql, Session session) throws Exception {
        Transaction txn = transactionManager.begin();
        try {
            String normalizedSql = normalizeSqlForMysql(sql);

            Lexer lexer = new Lexer(normalizedSql);
            Parser parser = new Parser(lexer.tokenize());
            StatementNode ast = parser.parse();
            if (ast == null) {
                transactionManager.abort(txn);
                return null;
            }
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast,session);
            PlanNode plan = planner.createPlan(ast);
            TupleIterator iterator = executionEngine.execute(plan, txn);
            transactionManager.commit(txn);
            return iterator;
        } catch (Exception e) {
            transactionManager.abort(txn);
            throw e;
        }
    }

    private String normalizeSqlForMysql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return sql;
        }

        String trimmedSql = sql.trim();

        if (!trimmedSql.endsWith(";")) {
            trimmedSql += ";";
        }

        return trimmedSql;
    }

    public TupleIterator createExecutorForQuery(String sql, Transaction txn, Session session) throws Exception {
        String normalizedSql = normalizeSqlForMysql(sql);

        Lexer lexer = new Lexer(normalizedSql);
        Parser parser = new Parser(lexer.tokenize());
        StatementNode ast = parser.parse();
        if (ast == null) {
            return null;
        }

        if (ast instanceof CreateDatabaseStatementNode || ast instanceof ShowDatabasesStatementNode) {
            PlanNode plan = planner.createPlan(ast);
            return executionEngine.execute(plan, null);
        }

        SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
        semanticAnalyzer.analyze(ast,session);
        PlanNode plan = planner.createPlan(ast);
        return executionEngine.execute(plan, txn);
    }

    public void execute(String sql){
        String result = executeAndGetResult(sql);
        System.out.println(result);
    }
}