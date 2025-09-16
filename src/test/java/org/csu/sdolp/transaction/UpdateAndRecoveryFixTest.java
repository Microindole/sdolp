// src/test/java/org/csu/sdolp/UpdateAndRecoveryFixTest.java
package org.csu.sdolp.transaction;

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
     * 核心测试场景：验证主键约束和崩溃恢复的健壮性
     */
    @Test
    void testPrimaryKeyViolationOnUpdateAndRecovery() throws IOException {
        System.out.println("\n--- SCENARIO: Testing PK Violation on UPDATE and CRASH RECOVERY ---");

        // --- 步骤 1: 第一次会话，设置初始数据 ---
        System.out.println("[Step 1] Starting first session and setting up initial data...");
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
        queryProcessor.execute("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR);");
        queryProcessor.execute("INSERT INTO employees (id, name) VALUES (1, 'Alice');");
        queryProcessor.execute("INSERT INTO employees (id, name) VALUES (2, 'Bob');");
        System.out.println("      Initial data created.");

        // --- 步骤 2: 尝试违反主键约束 (预期失败) ---
        System.out.println("\n[Step 2] Attempting to update id=2 to id=1, which should violate PK constraint...");
        String violatingUpdateSql = "UPDATE employees SET id = 1 WHERE id = 2;";
        String result = queryProcessor.executeAndGetResult(violatingUpdateSql);
        System.out.println("      Result: " + result);
        assertTrue(result.contains("Primary key constraint violation"), "Update should be rejected due to PK violation.");
        System.out.println("      Verification PASSED. PK violation was correctly caught.");

        // --- 步骤 3: 执行一个合法的 UPDATE 并模拟崩溃 ---
        System.out.println("\n[Step 3] Performing a valid update and then simulating a crash...");
        queryProcessor.execute("UPDATE employees SET name = 'Alicia' WHERE id = 1;");
        // --- 此处模拟崩溃：不调用 queryProcessor.close() ---
        queryProcessor = null; // 释放引用，防止在 tearDown 中被关闭
        System.out.println("      CRASH! (Log file is preserved)");
        
        File logFile = new File(TEST_LOG_FILE);
        assertTrue(logFile.exists(), "Log file must exist after a simulated crash to allow recovery.");

        // --- 步骤 4: 重启并触发恢复 ---
        System.out.println("\n[Step 4] Restarting database, RecoveryManager will be triggered...");
        // 创建一个新的QP实例，其构造函数会自动运行恢复流程
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // --- 步骤 5: 验证恢复后的数据 ---
        System.out.println("\n[Step 5] Verifying data after recovery...");
        String finalResult = queryProcessor.executeAndGetResult("SELECT * FROM employees ORDER BY id;");
        System.out.println("      Query result after recovery:\n" + finalResult);

        assertTrue(finalResult.contains("Alicia"), "Recovered name 'Alicia' should be present.");
        assertFalse(finalResult.contains("'Alice'"), "Original name 'Alice' should NOT be present after redo.");
        assertTrue(finalResult.contains("Bob"), "Unaffected record 'Bob' should still be present.");
        assertTrue(finalResult.contains("2 rows returned"), "There should be exactly 2 rows after recovery.");

        // 关键验证：确保没有因为错误的Redo逻辑产生重复数据
        long countOfAlicia = finalResult.lines().filter(line -> line.contains("Alicia")).count();
        assertEquals(1, countOfAlicia, "There should be only ONE record with the name 'Alicia'. Duplicates indicate a recovery bug.");
        
        System.out.println("\n[SUCCESS] All verifications passed. The UPDATE and recovery logic is now correct!");
    }
}