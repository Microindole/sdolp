// src/test/java/org/csu/sdolp/UpdateAndRecoveryFixTest.java
package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于验证 UPDATE 逻辑修复 和 正常/异常关闭后数据一致性的集成测试.
 */
public class UpdateAndRecoveryFixTest {

    private final String TEST_DB_NAME = "update_fix_test_db";
    private final String TEST_DB_FILE = "data/" + TEST_DB_NAME + "/minidb.data";
    private final String TEST_LOG_FILE = "data/" + TEST_DB_NAME + "/minidb.data.log";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // 每次测试前，确保环境是干净的
        System.out.println("--- [TEST SETUP] Cleaning up environment for database: " + TEST_DB_NAME);
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @AfterEach
    void tearDown() throws IOException {
        // 确保测试后也清理环境
         System.out.println("--- [TEST TEARDOWN] Cleaning up environment ---");
         if (queryProcessor != null) {
             queryProcessor.close();
         }
         deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    private void deleteDirectory(File directory) {
        if (!directory.exists()) {
            return;
        }
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    /**
     * 测试场景A: 验证正常关闭流程
     * 1. 执行 DML 操作 (CREATE, INSERT, UPDATE).
     * 2. 正常关闭数据库 (调用 close()).
     * 3. 验证日志文件是否被删除.
     * 4. 重启数据库.
     * 5. 查询数据，验证数据是否正确且没有重复.
     */
    @Test
    void testUpdateConsistencyAfterGracefulShutdown() throws IOException {
        System.out.println("\n--- SCENARIO A: Testing Correctness After GRACEFUL SHUTDOWN ---");

        // --- 步骤 1: 第一次会话 ---
        System.out.println("[A-1] Starting first session and performing operations...");
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
        queryProcessor.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR);");
        queryProcessor.execute("INSERT INTO employees (id, name) VALUES (1, 'Alice');");
        queryProcessor.execute("INSERT INTO employees (id, name) VALUES (2, 'Bob');");
        queryProcessor.execute("UPDATE employees SET name = 'Alicia' WHERE id = 1;");

        // --- 步骤 2: 正常关闭 ---
        System.out.println("[A-2] Closing database gracefully...");
        queryProcessor.close();
        queryProcessor = null; // 确保旧实例被释放

        // --- 步骤 3: 验证日志文件 ---
        File logFile = new File(TEST_LOG_FILE);
        System.out.println("[A-3] Verifying log file is deleted: " + TEST_LOG_FILE);
        assertFalse(logFile.exists(), "Log file should be deleted after a graceful shutdown.");
        System.out.println("      Verification PASSED. Log file does not exist.");

        // --- 步骤 4 & 5: 第二次会话，验证数据 ---
        System.out.println("[A-4] Restarting database and verifying data...");
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
        String result = queryProcessor.executeAndGetResult("SELECT * FROM employees ORDER BY id;");
        System.out.println("      Query result after restart:\n" + result);

        assertTrue(result.contains("Alicia"), "Updated name 'Alicia' should be present.");
        assertFalse(result.contains("Alice'"), "Original name 'Alice' should NOT be present.");
        assertTrue(result.contains("2 rows returned"), "There should be exactly 2 rows in the table.");

        // 额外检查，确保没有重复的 'Alicia'
        long countOfAlicia = result.lines().filter(line -> line.contains("Alicia")).count();
        assertEquals(1, countOfAlicia, "There should be only one record with the name 'Alicia'.");
        
        System.out.println("[A-5] Data verification PASSED. Test for graceful shutdown is successful!");
    }

    /**
     * 测试场景B: 模拟崩溃后的恢复流程
     * 这是一个简化的模拟，利用您现有的 RecoveryTest 逻辑。
     * 它验证的是：如果一个 UPDATE 操作写入了日志但数据库崩溃了，
     * 重启后的恢复流程(Redo)会不会造成数据不一致。
     * * 注意：这个测试并没有验证未提交事务的回滚(Undo)，但可以验证Redo的正确性。
     */
    @Test
    void testUpdateRedoConsistencyAfterCrash() throws IOException {
        System.out.println("\n--- SCENARIO B: Testing Correctness After CRASH (Redo phase) ---");

        // --- 步骤 1: 执行操作并模拟崩溃 ---
        // 我们通过创建一个QP实例，执行操作，然后不调用close()就结束方法来模拟崩溃。
        // 这样日志文件会保留下来。
        System.out.println("[B-1] Simulating a session that crashes...");
        QueryProcessor crashingProcessor = new QueryProcessor(TEST_DB_NAME);
        crashingProcessor.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR);");
        crashingProcessor.execute("INSERT INTO employees (id, name) VALUES (1, 'Alice');");
        crashingProcessor.execute("INSERT INTO employees (id, name) VALUES (2, 'Bob');");
        crashingProcessor.execute("UPDATE employees SET name = 'Alicia' WHERE id = 1;");
        // --- 此处模拟崩溃：不调用 crashingProcessor.close() ---
        System.out.println("      CRASH! (Log file is preserved)");

        File logFile = new File(TEST_LOG_FILE);
        assertTrue(logFile.exists(), "Log file must exist after a simulated crash.");

        // --- 步骤 2: 重启并触发恢复 ---
        System.out.println("[B-2] Restarting database, RecoveryManager will be triggered...");
        // 创建一个新的QP实例，其构造函数会自动运行恢复流程
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // --- 步骤 3: 验证数据 ---
        System.out.println("[B-3] Verifying data after recovery...");
        String result = queryProcessor.executeAndGetResult("SELECT * FROM employees ORDER BY id;");
        System.out.println("      Query result after recovery:\n" + result);

        assertTrue(result.contains("Alicia"), "Recovered name 'Alicia' should be present.");
        assertFalse(result.contains("Alice'"), "Original name 'Alice' should NOT be present after redo.");
        assertTrue(result.contains("2 rows returned"), "There should be exactly 2 rows after recovery.");

        long countOfAlicia = result.lines().filter(line -> line.contains("Alicia")).count();
        assertEquals(1, countOfAlicia, "There should be only one record with 'Alicia' after recovery.");
        
        System.out.println("[B-4] Data verification PASSED. Test for crash recovery (Redo) is successful!");
    }
}