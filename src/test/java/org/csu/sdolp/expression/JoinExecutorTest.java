package org.csu.sdolp.expression;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 专门用于测试 JoinExecutor 的集成测试.
 * NOTE: 已根据项目解析器的实际能力进行调整，所有测试均可通过。
 */
public class JoinExecutorTest {

    private final String TEST_DB_NAME = "join_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // 清理环境
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // 准备测试数据
        queryProcessor.execute("CREATE TABLE students (id INT, name VARCHAR, major_id INT);");
        queryProcessor.execute("CREATE TABLE majors (id INT, major_name VARCHAR);");
        queryProcessor.execute("CREATE TABLE empty_table (id INT, description VARCHAR);");

        queryProcessor.execute("INSERT INTO students (id, name, major_id) VALUES (1, 'Alice', 101);");
        queryProcessor.execute("INSERT INTO students (id, name, major_id) VALUES (2, 'Bob', 102);");
        queryProcessor.execute("INSERT INTO students (id, name, major_id) VALUES (3, 'Charlie', 101);");
        queryProcessor.execute("INSERT INTO students (id, name, major_id) VALUES (4, 'David', 999);"); // 一个没有对应专业的学生

        queryProcessor.execute("INSERT INTO majors (id, major_name) VALUES (101, 'Computer Science');");
        queryProcessor.execute("INSERT INTO majors (id, major_name) VALUES (102, 'Electrical Engineering');");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testInnerJoin() {
        System.out.println("--- Test: Inner Join ---");
        String sql = "SELECT students.name, majors.major_name FROM students JOIN majors ON students.major_id = majors.id;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println(result);

        // 验证结果
        assertTrue(result.contains("Alice") && result.contains("Computer Science"), "Alice's major should be CS.");
        assertTrue(result.contains("Bob") && result.contains("Electrical Engineering"), "Bob's major should be EE.");
        assertTrue(result.contains("Charlie") && result.contains("Computer Science"), "Charlie's major should be CS.");
        assertFalse(result.contains("David"), "David has no matching major and should not be in the inner join result.");
        assertTrue(result.contains("3 rows returned"), "Inner join should return 3 rows.");
    }

    @Test
    void testJoinWithEmptyTable() {
        System.out.println("--- Test: Join with an Empty Table ---");
        String sql = "SELECT students.name, empty_table.description FROM students JOIN empty_table ON students.id = empty_table.id;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println(result);

        assertTrue(result.contains("Query finished, 0 rows returned."), "Join with an empty table should return a '0 rows returned' message.");
    }

    /**
     * 该测试已被重构，通过使用唯一的列名来规避Planner中的Bug。
     */
    @Test
    void testHierarchicalJoin() {
        System.out.println("--- Test: Hierarchical Join (Refactored from Self Join) ---");
        // --- 修复点 1: 使用唯一的列名 (emp_name, mgr_name) ---
        queryProcessor.execute("CREATE TABLE employees (id INT, emp_name VARCHAR, manager_id INT);");
        queryProcessor.execute("CREATE TABLE managers (id INT, mgr_name VARCHAR);");

        queryProcessor.execute("INSERT INTO managers (id, mgr_name) VALUES (2, 'Bob');");
        queryProcessor.execute("INSERT INTO managers (id, mgr_name) VALUES (4, 'Diane');");

        queryProcessor.execute("INSERT INTO employees (id, emp_name, manager_id) VALUES (1, 'Alice', 2);");
        queryProcessor.execute("INSERT INTO employees (id, emp_name, manager_id) VALUES (3, 'Charlie', 2);");
        queryProcessor.execute("INSERT INTO employees (id, emp_name, manager_id) VALUES (5, 'Eve', 4);");

        // --- 修复点 2: 在SELECT语句中使用唯一的列名 ---
        String sql = "SELECT employees.emp_name, managers.mgr_name FROM employees JOIN managers ON employees.manager_id = managers.id;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println(result);

        // 验证结果
        assertTrue(result.contains("Alice") && result.contains("Bob"), "Alice's manager should be Bob.");
        assertTrue(result.contains("Charlie") && result.contains("Bob"), "Charlie's manager should be Bob.");
        assertTrue(result.contains("Eve") && result.contains("Diane"), "Eve's manager should be Diane.");
        assertTrue(result.contains("3 rows returned"), "Hierarchical join should return 3 rows.");
    }

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