package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于测试 HAVING 子句功能的集成测试.
 */
public class HavingClauseTest {

    private final String TEST_DB_NAME = "having_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // 在每次测试前清理并重新创建数据库实例
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // 创建并填充测试数据
        queryProcessor.executeAndGetResult("CREATE TABLE employees (id INT, department VARCHAR(50), salary INT);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (1, 'Engineering', 70000);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (2, 'Engineering', 80000);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (3, 'HR', 50000);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (4, 'Sales', 90000);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (5, 'Sales', 100000);");
        queryProcessor.executeAndGetResult("INSERT INTO employees (id, department, salary) VALUES (6, 'Sales', 110000);");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testHavingWithAvg() {
        String sql = "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 70000;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("--- Test: HAVING with AVG(salary) > 70000 ---");
        System.out.println(result);

        // 预期结果：只有 Engineering (75000) 和 Sales (100000) 部门满足条件
        assertTrue(result.contains("Engineering"), "Result should contain Engineering department.");
        assertTrue(result.contains("Sales"), "Result should contain Sales department.");
        assertFalse(result.contains("HR"), "Result should NOT contain HR department.");
        assertTrue(result.contains("75000"), "Result should contain average salary for Engineering.");
        assertTrue(result.contains("100000"), "Result should contain average salary for Sales.");
    }

    @Test
    void testHavingWithCount() {
        String sql = "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 2;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("--- Test: HAVING with COUNT(*) > 2 ---");
        System.out.println(result);

        // 预期结果：只有 Sales 部门 (3人) 满足条件
        assertTrue(result.contains("Sales"), "Result should contain Sales department.");
        assertFalse(result.contains("Engineering"), "Result should NOT contain Engineering department.");
        assertFalse(result.contains("HR"), "Result should NOT contain HR department.");
        assertTrue(result.contains("3"), "Result should contain the count for Sales department.");
    }

    @Test
    void testHavingWithColumnInGroupBy() {
        String sql = "SELECT department FROM employees GROUP BY department HAVING department = 'HR';";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("--- Test: HAVING with a GROUP BY column ---");
        System.out.println(result);

        // 预期结果：只返回 HR 部门
        assertTrue(result.contains("HR"), "Result should contain HR department.");
        assertFalse(result.contains("Engineering"), "Result should NOT contain Engineering department.");
        assertFalse(result.contains("Sales"), "Result should NOT contain Sales department.");
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
}

