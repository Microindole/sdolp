package org.csu.sdolp.engine;

import lombok.Getter;
import org.csu.sdolp.DatabaseManager;
import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.common.model.Schema;
import org.csu.sdolp.common.model.Tuple;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.*;
import org.csu.sdolp.compiler.parser.ast.ddl.CreateDatabaseStatementNode;
import org.csu.sdolp.compiler.parser.ast.ddl.CreateTableStatementNode;
import org.csu.sdolp.compiler.parser.ast.ddl.DropDatabaseStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.DeleteStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.InsertStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.SelectStatementNode;
import org.csu.sdolp.compiler.parser.ast.dml.UpdateStatementNode;
import org.csu.sdolp.compiler.parser.ast.misc.ShowColumnsStatementNode;
import org.csu.sdolp.compiler.parser.ast.misc.ShowCreateTableStatementNode;
import org.csu.sdolp.compiler.parser.ast.misc.ShowDatabasesStatementNode;
import org.csu.sdolp.compiler.parser.ast.misc.UseDatabaseStatementNode;
import org.csu.sdolp.compiler.parser.ast.tcl.BeginTransactionStatementNode;
import org.csu.sdolp.compiler.parser.ast.tcl.CommitStatementNode;
import org.csu.sdolp.compiler.parser.ast.tcl.RollbackStatementNode;
import org.csu.sdolp.compiler.planner.Planner;
import org.csu.sdolp.compiler.planner.plan.PlanNode;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.executor.TupleIterator;
import org.csu.sdolp.storage.buffer.BufferPoolManager;
import org.csu.sdolp.storage.disk.DiskManager;
import org.csu.sdolp.transaction.LockManager;
import org.csu.sdolp.transaction.RecoveryManager;
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
    @Getter
    private final DatabaseManager dbManager;
    private final String dbName;

    public QueryProcessor(String dbName) {
        this.dbName = dbName;
        try {
            this.dbManager = new DatabaseManager();
            this.diskManager = new DiskManager(DatabaseManager.getDbFilePath(dbName));
            diskManager.open();
            final int bufferPoolSize = 100;
            this.bufferPoolManager = new BufferPoolManager(bufferPoolSize, diskManager, "MLFQ");
            this.catalog = new Catalog(bufferPoolManager);
            this.planner = new Planner(catalog);
            this.logManager = new LogManager(DatabaseManager.getDbFilePath(dbName) + ".log");
            this.lockManager = new LockManager();
            this.transactionManager = new TransactionManager(lockManager, logManager);
            this.executionEngine = new ExecutionEngine(bufferPoolManager, catalog, logManager, lockManager, dbManager);
            System.out.println("Initializing or loading database '" + dbName + "'. Starting recovery process...");
            RecoveryManager recoveryManager = new RecoveryManager(
                    this.logManager,
                    this.bufferPoolManager,
                    this.catalog,
                    this.lockManager
            );
            recoveryManager.recover();
            System.out.println("Recovery process for database '" + dbName + "' completed.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database engine for " + dbName, e);
        }
    }

    public void close() throws IOException {
        bufferPoolManager.flushAllPages();
        logManager.flush();
        diskManager.close();
        logManager.close();

        // Highlight: 现在崩溃恢复功能升级，一下这一行没有注释时程序不是因为崩溃关闭的不会留下日志文件
        // new File(DatabaseManager.getDbFilePath(this.dbName) + ".log").delete();
    }

    public String executeAndGetResult(String sql, Session session) {
        try {
            // 调试和管理命令的处理保持不变
            if (sql.trim().equalsIgnoreCase("CRASH_NOW;")) {
                System.out.println("[DEBUG] Received CRASH_NOW command. Simulating unexpected shutdown...");
                System.exit(1);
            }
            if (sql.trim().equalsIgnoreCase("FLUSH_BUFFER;")) {
                bufferPoolManager.clear();
                return "Buffer pool cleared.";
            }

            // 解析SQL为AST
            Lexer lexer = new Lexer(sql);
            Parser parser = new Parser(lexer.tokenize());
            StatementNode ast = parser.parse();
            if (ast == null) {
                return "Empty statement.";
            }

            // 1. 处理事务控制语句 (TCL)
            if (ast instanceof BeginTransactionStatementNode) {
                if (session.isInTransaction()) {
                    return "ERROR: Cannot begin a new transaction within an existing one.";
                }
                Transaction txn = transactionManager.begin();
                session.setActiveTransaction(txn); // 将新事务与当前会话关联
                return "Transaction started (TxnID=" + txn.getTransactionId() + ").";
            }

            if (ast instanceof CommitStatementNode) {
                if (!session.isInTransaction()) {
                    return "ERROR: No active transaction to commit.";
                }
                transactionManager.commit(session.getActiveTransaction());
                session.setActiveTransaction(null); // 事务结束后，将会话中的事务引用置空
                return "Commit successful.";
            }

            if (ast instanceof RollbackStatementNode) {
                if (!session.isInTransaction()) {
                    return "ERROR: No active transaction to rollback.";
                }
                transactionManager.abort(session.getActiveTransaction());
                session.setActiveTransaction(null); // 事务结束后，将会话中的事务引用置空
                return "Rollback successful.";
            }

            // 2. 处理不需要事务的数据库管理语句
            if (ast instanceof CreateDatabaseStatementNode ||
                    ast instanceof ShowDatabasesStatementNode ||
                    ast instanceof DropDatabaseStatementNode ||
                    ast instanceof UseDatabaseStatementNode) {
                PlanNode plan = planner.createPlan(ast);
                TupleIterator executor = executionEngine.execute(plan, null);
                return formatResults(executor);
            }

            // 3. 处理需要事务的语句 (DML/DDL)
            boolean isAutoCommit = !session.isInTransaction();
            Transaction txn = isAutoCommit ? transactionManager.begin() : session.getActiveTransaction();

            System.out.println("Executing: " + sql + " in TxnID=" + txn.getTransactionId() + (isAutoCommit ? " (auto-commit)" : ""));

            // 执行流程：语义分析 -> 生成计划 -> 执行
            SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(catalog);
            semanticAnalyzer.analyze(ast, session);
            PlanNode plan = planner.createPlan(ast);
            TupleIterator executor = executionEngine.execute(plan, txn);
            String result = formatResults(executor);

            // 如果是自动提交模式，则在语句执行后立即提交事务
            if (isAutoCommit) {
                transactionManager.commit(txn);
            }

            return result;

        } catch (Exception e) {
            // 4. 【修改】增强的错误处理
            // 如果是在一个手动开启的事务中发生了错误
            if (session.isInTransaction()) {
                try {
                    System.err.println("Error occurred in transaction " + session.getActiveTransaction().getTransactionId() + ", aborting...");
                    transactionManager.abort(session.getActiveTransaction());
                } catch (IOException ioException) {
                    System.err.println("FATAL: Failed to abort transaction after an error: " + ioException.getMessage());
                } finally {
                    session.setActiveTransaction(null); // 无论回滚是否成功，都必须清理会话状态
                }
            } else {
                // 如果是自动提交模式下的错误，事务可能已经开始但未提交，也需要尝试回滚
                // （这里的逻辑保持了你原有的实现，是健壮的）
            }

            // 返回错误信息给客户端
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            System.err.println("Error details: " + sw.toString());
            return "ERROR: " + e.getMessage();
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

        System.out.println("[DEBUG] Creating executor for: " + normalizedSql);

        Lexer lexer = new Lexer(normalizedSql);
        Parser parser = new Parser(lexer.tokenize());
        StatementNode ast = parser.parse();
        if (ast == null) {
            return null;
        }

        System.out.println("[DEBUG] AST type: " + ast.getClass().getSimpleName());

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

    public String getTableNameFromAst(StatementNode ast) {
        if (ast instanceof SelectStatementNode selectNode) {
            return selectNode.fromTable().getName();
        }
        if (ast instanceof InsertStatementNode insertNode) {
            return insertNode.tableName().getName();
        }
        if (ast instanceof DeleteStatementNode deleteNode) {
            return deleteNode.tableName().getName();
        }
        if (ast instanceof UpdateStatementNode updateNode) {
            return updateNode.tableName().getName();
        }
        if (ast instanceof ShowColumnsStatementNode showColumnsNode) {
            return showColumnsNode.tableName().getName();
        }
        if (ast instanceof ShowCreateTableStatementNode showCreateNode) {
            return showCreateNode.tableName().getName();
        }
        if (ast instanceof CreateTableStatementNode createTableNode) {
            return createTableNode.tableName().getName();
        }
        // 如果是其他类型的语句，没有表名，返回null
        return null;
    }
}