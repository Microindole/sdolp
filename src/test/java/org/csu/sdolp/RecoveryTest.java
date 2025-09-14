package org.csu.sdolp;

import org.csu.sdolp.catalog.Catalog;
import org.csu.sdolp.cli.server.Session;
import org.csu.sdolp.compiler.lexer.Lexer;
import org.csu.sdolp.compiler.parser.Parser;
import org.csu.sdolp.compiler.parser.ast.StatementNode;
import org.csu.sdolp.compiler.planner.plan.PlanNode;
import org.csu.sdolp.compiler.semantic.SemanticAnalyzer;
import org.csu.sdolp.engine.QueryProcessor;
import org.csu.sdolp.transaction.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于测试崩溃恢复功能的集成测试.
 * 严格遵循 "Setup -> Crash -> Recover -> Verify" 的流程.
 */
public class RecoveryTest {

    private final String TEST_DB_FILE = "recovery_test.db";
    private final String TEST_LOG_FILE = "recovery_test.db.log";
    private QueryProcessor queryProcessorForVerification;

    @BeforeEach
    void setUp() {
        // 每次测试前，确保环境是干净的
        System.out.println("--- [RecoveryTest] Cleaning up environment ---");
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessorForVerification != null) {
            queryProcessorForVerification.close();
        }
        // 测试结束后再次清理
        new File(TEST_DB_FILE).delete();
        new File(TEST_LOG_FILE).delete();
        System.out.println("--- [RecoveryTest] Environment cleaned up ---");
    }

    @Test
    void testRecoveryAfterCrashDuringTransaction() throws Exception {
        System.out.println("--- [TEST] Starting recovery test for uncommitted transaction ---");

        // --- 步骤 1: Setup ---
        // 使用一个临时的 QueryProcessor 来创建表和已提交的初始数据
        System.out.println("--- [Step 1] Executing committed transactions for setup ---");
        QueryProcessor setupProcessor = new QueryProcessor(TEST_DB_FILE);
        setupProcessor.execute("CREATE TABLE recovery_test (id INT, status VARCHAR);");
        setupProcessor.execute("INSERT INTO recovery_test (id, status) VALUES (1, 'committed_A');");
        setupProcessor.execute("INSERT INTO recovery_test (id, status) VALUES (2, 'committed_B');");
        setupProcessor.close(); // 干净地关闭，确保所有数据都已持久化
        System.out.println("--- Committed setup transactions finished ---");


        // --- 步骤 2 & 3: Crash ---
        System.out.println("--- [Step 2 & 3] Simulating an interrupted transaction ---");
        // 创建一个新的实例来执行注定要“失败”的事务
        QueryProcessor crashingProcessor = new QueryProcessor(TEST_DB_FILE);

        // 使用反射来手动开启一个事务，绕过自动提交
        Object transactionManager = getField(crashingProcessor, "transactionManager");
        Method beginMethod = transactionManager.getClass().getMethod("begin");
        Transaction uncommittedTxn = (Transaction) beginMethod.invoke(transactionManager);
        System.out.println("--- Manually started TxnID=" + uncommittedTxn.getTransactionId() + " ---");

        // 在这个手动开启的事务中执行两个操作
        executeSqlInTransaction(crashingProcessor, "UPDATE recovery_test SET status = 'updated_but_crashed' WHERE id = 2;", uncommittedTxn);
        executeSqlInTransaction(crashingProcessor, "INSERT INTO recovery_test (id, status) VALUES (3, 'inserted_but_crashed');", uncommittedTxn);

        System.out.println("--- Uncommitted operations logged. Simulating CRASH now (by not committing and not closing) ---");
        // **关键点**: 我们不调用 commit，也不调用 crashingProcessor.close()。
        // 这就完美模拟了数据库进程在事务提交前突然被终止的场景。
        // crashingProcessor 实例将被 Java 垃圾回收，但它写入的日志文件和可能已刷盘的数据页留了下来。


        // --- 步骤 4: Recover & Verify ---
        System.out.println("--- [Step 4] Restarting server, RecoveryManager will be invoked ---");
        // 创建第三个 QueryProcessor 实例。它的构造函数会自动运行恢复流程。
        // 这个实例将作为我们最终的验证环境。
        queryProcessorForVerification = new QueryProcessor(TEST_DB_FILE);

        System.out.println("--- [Step 5] Verifying data after recovery ---");
        String result = queryProcessorForVerification.executeAndGetResult("SELECT * FROM recovery_test ORDER BY id;");
        System.out.println("Query Result after recovery:\n" + result);

        // --- 最终预期结果断言 ---
        assertTrue(result.contains("committed_A"), "Should contain committed data 'committed_A'");
        assertTrue(result.contains("committed_B"), "Should contain original 'committed_B' because UPDATE was rolled back");
        assertFalse(result.contains("updated_but_crashed"), "Should NOT contain rolled-back updated data");
        assertFalse(result.contains("inserted_but_crashed"), "Should NOT contain rolled-back inserted data");
        assertTrue(result.contains("2 rows returned"), "Should only have 2 rows after recovery, because INSERT was rolled back");

        System.out.println("--- [TEST] Recovery test PASSED ---");
    }

    /**
     * 一个辅助方法，使用反射来执行一条SQL语句在一个指定的、手动开启的事务中。
     * @param qp 当前的 QueryProcessor 实例
     * @param sql 要执行的 SQL
     * @param txn 所在的事务
     */
    private void executeSqlInTransaction(QueryProcessor qp, String sql, Transaction txn) throws Exception {
        // 通过反射获取 QueryProcessor 内部的组件
        Object planner = getField(qp, "planner");
        Object catalog = getField(qp, "catalog");
        Object executionEngine = getField(qp, "executionEngine");

        // 1. 词法分析 -> 语法分析 -> AST
        Lexer lexer = new Lexer(sql);
        Parser parser = new Parser(lexer.tokenize());
        StatementNode ast = parser.parse();

        // 2. 语义分析
        SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer((Catalog) catalog);
        Session mockRootSession = Session.createAuthenticatedSession(txn.getTransactionId(), "root");
        semanticAnalyzer.analyze(ast,mockRootSession);

        // 3. 计划生成
        Method createPlanMethod = planner.getClass().getMethod("createPlan", StatementNode.class);
        PlanNode plan = (PlanNode) createPlanMethod.invoke(planner, ast);

        // 4. 执行
        Method executeMethod = executionEngine.getClass().getMethod("execute", PlanNode.class, Transaction.class);
        executeMethod.invoke(executionEngine, plan, txn);
        System.out.println("Executed in Txn: " + sql);
    }

    /**
     * 一个通用的反射工具，用于获取一个对象的私有成员变量。
     * @param obj 目标对象
     * @param fieldName 成员变量名
     * @return 成员变量的值
     */
    private Object getField(Object obj, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}