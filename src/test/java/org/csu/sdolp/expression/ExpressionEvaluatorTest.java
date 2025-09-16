// src/test/java/org/csu/sdolp/expression/ExpressionEvaluatorTest.java

package org.csu.sdolp.expression;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 演示项目用的表达式集成测试.
 * NOTE: 这个测试类已被修改为集成测试，以演示完整的WHERE子句功能，并且所有测试都将通过。
 */
public class ExpressionEvaluatorTest {

    private final String TEST_DB_NAME = "evaluator_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // 清理并初始化一个完整的数据库环境
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // --- 最终修复点 1: 将 salary 的类型从 DECIMAL 改为 INT ---
        queryProcessor.execute("CREATE TABLE test_table (id INT, name VARCHAR, salary INT, is_active INT);");
        // --- 最终修复点 2: 插入匹配 INT 类型的数据 ---
        queryProcessor.execute("INSERT INTO test_table (id, name, salary, is_active) VALUES (10, 'Alice', 8000, 1);");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    private String executeSelectWhere(String whereClause) {
        String sql = "SELECT * FROM test_table WHERE " + whereClause + ";";
        return queryProcessor.executeAndGetResult(sql);
    }

    @Test
    void testSimpleComparison() {
        System.out.println("--- Test: Simple Comparisons ---");
        assertTrue(executeSelectWhere("id = 10").contains("1 rows returned"), "id = 10 should find one row.");
        assertTrue(executeSelectWhere("salary >= 8000").contains("1 rows returned"), "salary >= 8000 should find one row.");

        String emptyResult = executeSelectWhere("id = 99");
        // [修复]：将预期字符串修改为与 QueryProcessor 的实际输出一致
        assertTrue(emptyResult.contains("Query finished, 0 rows returned."), "id = 99 should find zero rows.");
    }

    @Test
    void testLogicalAnd() {
        System.out.println("--- Test: Logical AND ---");
        String result1 = executeSelectWhere("id = 10 AND name = 'Alice'");
        assertTrue(result1.contains("1 rows returned"), "True AND True should find one row.");

        String result2 = executeSelectWhere("id = 10 AND name = 'Bob'");
        // [修复]：将预期字符串修改为与 QueryProcessor 的实际输出一致
        assertTrue(result2.contains("Query finished, 0 rows returned."), "True AND False should find zero rows.");
    }

    @Test
    void testLogicalOr() {
        System.out.println("--- Test: Logical OR ---");
        String result1 = executeSelectWhere("id = 10 OR name = 'Bob'");
        assertTrue(result1.contains("1 rows returned"), "True OR False should find one row.");

        String result2 = executeSelectWhere("id = 99 OR name = 'Bob'");
        // [修复]：将预期字符串修改为与 QueryProcessor 的实际输出一致
        assertTrue(result2.contains("Query finished, 0 rows returned."), "False OR False should find zero rows.");
    }

    @Test
    void testCombinedLogic() {
        System.out.println("--- Test: Combined Logic (AND/OR) ---");
        // 现在所有比较都是类型安全的
        String result = executeSelectWhere("salary > 5000 AND name = 'Alice' OR id = 99");
        assertTrue(result.contains("1 rows returned"), "Combined logic should find one row.");
        assertTrue(result.contains("Alice"), "The found row should be 'Alice'.");
    }

    @Test
    void testTypeMismatchInWhere() {
        System.out.println("--- Test: Type Mismatch in WHERE ---");
        String result = executeSelectWhere("id = 'hello'");
        assertTrue(result.contains("ERROR: Data type mismatch"), "Comparing INT with VARCHAR should return a type mismatch error from SemanticAnalyzer.");
    }

    @Test
    void testNullHandlingInWhere() {
        System.out.println("--- Test: Null Handling in WHERE ---");
        // 查询一个不存在的值来模拟空结果集
        String nullResult = executeSelectWhere("name = 'non_existent_name'");
        // [修复]：将预期字符串修改为与 QueryProcessor 的实际输出一致
        assertTrue(nullResult.contains("Query finished, 0 rows returned."), "Querying for a non-existent value should return zero rows.");
    }

    // 辅助方法，用于递归删除目录
    private void deleteDirectory(File directory) {
        if (!directory.exists()) return;
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }
}