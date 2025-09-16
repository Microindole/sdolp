package org.csu.sdolp.compiler;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SQL语句集成一些关键字的测试
 * Order by,limit,Complex where with and/or,
 * alter,drop,show,aggregation(group by,sum,count......)
 */
public class KeyWordsTest {

    private final String TEST_DB_NAME = "system_integration_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // --- 核心修复点 2: 调用 deleteDirectory 清理整个数据库目录 ---
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        // --- 核心修复点 3: 同样，在测试后也清理整个目录 ---
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    // --- 核心修复点 4: 新增递归删除目录的辅助方法 ---
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


    @Test
    void testOrderByAndLimitFlow() {
        System.out.println("\n--- Test: Order By and Limit ---");
        queryProcessor.execute("CREATE TABLE employees (id INT, name VARCHAR, age INT);");
        queryProcessor.execute("INSERT INTO employees (id, name, age) VALUES (1, 'Eve', 22);");
        queryProcessor.execute("INSERT INTO employees (id, name, age) VALUES (2, 'Frank', 35);");
        queryProcessor.execute("INSERT INTO employees (id, name, age) VALUES (3, 'Grace', 28);");
        queryProcessor.execute("INSERT INTO employees (id, name, age) VALUES (4, 'Dave', 22);");

        System.out.println("\n--- Selecting all employees, ordered by age DESC ---");
        System.out.println("--- Expected order: Frank(35), Grace(28), Eve(22), Dave(22) ---");
        queryProcessor.execute("SELECT * FROM employees ORDER BY age DESC;");

        System.out.println("\n--- Selecting all employees, ordered by age ASC, limited to 2 ---");
        System.out.println("--- Expected order: Eve(22), Dave(22) (or vice versa), then Grace(28) ---");
        System.out.println("--- Expected result: First two employees (age 22 and 28) ---");
        queryProcessor.execute("SELECT * FROM employees ORDER BY age ASC LIMIT 2;");
    }
    @Test
    void testComplexWhereClauseFlow() {
        System.out.println("\n--- Test: Complex WHERE clause with AND/OR ---");
        queryProcessor.execute("CREATE TABLE students (id INT, name VARCHAR, age INT, major VARCHAR);");
        queryProcessor.execute("INSERT INTO students (id, name, age, major) VALUES (1, 'Alice', 20, 'CS');");
        queryProcessor.execute("INSERT INTO students (id, name, age, major) VALUES (2, 'Bob', 22, 'EE');");
        queryProcessor.execute("INSERT INTO students (id, name, age, major) VALUES (3, 'Charlie', 20, 'CS');");
        queryProcessor.execute("INSERT INTO students (id, name, age, major) VALUES (4, 'David', 23, 'CS');");

        System.out.println("\n--- Selecting CS students with age = 20 (Using AND) ---");
        System.out.println("--- Expected: Alice, Charlie ---");
        queryProcessor.execute("SELECT name FROM students WHERE age = 20 AND major = 'CS';");

        System.out.println("\n--- Selecting EE students OR students older than 22 (Using OR) ---");
        System.out.println("--- Expected: Bob, David ---");
        queryProcessor.execute("SELECT name FROM students WHERE major = 'EE' OR age > 22;");
    }
    @Test
    void testDdlFlow() {
        System.out.println("\n--- Test: DDL Flow (ALTER, DROP) ---");

        // 1. 创建一个初始表
        queryProcessor.execute("CREATE TABLE to_be_modified (id INT, name VARCHAR);");
        queryProcessor.execute("INSERT INTO to_be_modified (id, name) VALUES (1, 'initial');");

        System.out.println("\n--- Table before ALTER ---");
        queryProcessor.execute("SELECT * FROM to_be_modified;");

        // 2. 使用 ALTER TABLE 添加新列
        System.out.println("\n--- Altering table to add 'score' column ---");
        queryProcessor.execute("ALTER TABLE to_be_modified ADD COLUMN score INT;");

        // 3. 插入包含新列的数据
        queryProcessor.execute("INSERT INTO to_be_modified (id, name, score) VALUES (2, 'added', 100);");

        System.out.println("\n--- Table after ALTER and new INSERT ---");
        // 注意：第一次插入的 'initial' 行，其 score 列应该是 null。我们的简化模型会如何表现？
        // 在我们的 Tuple/Value 模型下，它会是 null object in value list.
        // select * 应该能正常工作并显示三列
        queryProcessor.execute("SELECT * FROM to_be_modified;");

        // 4. 使用 DROP TABLE 删除表
        System.out.println("\n--- Dropping table ---");
        queryProcessor.execute("DROP TABLE to_be_modified;");

        // 5. 验证表已被删除（预期会收到 "Table not found" 错误）
        System.out.println("\n--- Verifying table is dropped (expecting an error) ---");
        queryProcessor.execute("SELECT * FROM to_be_modified;");
    }
    @Test
    void testShowTablesFlow() {
        System.out.println("--- Test: SHOW TABLES ---");

        // 1. Create two tables
        queryProcessor.execute("CREATE TABLE table_b (id INT);");
        queryProcessor.execute("CREATE TABLE table_a (id INT);");

        // 2. Execute SHOW TABLES and verify the result
        System.out.println("\n--- SHOW TABLES after creating two tables ---");
        String showResult = queryProcessor.executeAndGetResult("SHOW TABLES;");
        System.out.println(showResult);

        // Assert that the output contains both table names (sorted alphabetically)
        assertTrue(showResult.contains("table_a"), "SHOW TABLES result should contain 'table_a'");
        assertTrue(showResult.contains("table_b"), "SHOW TABLES result should contain 'table_b'");
        assertTrue(showResult.contains("2 rows returned"), "Should indicate 2 rows were returned");


        // 3. Drop one table and verify again
        queryProcessor.execute("DROP TABLE table_a;");
        System.out.println("\n--- SHOW TABLES after dropping one table ---");
        String finalShowResult = queryProcessor.executeAndGetResult("SHOW TABLES;");
        System.out.println(finalShowResult);

        // Assert that the dropped table is gone and the other remains
        assertFalse(finalShowResult.contains("table_a"), "SHOW TABLES result should no longer contain 'table_a'");
        assertTrue(finalShowResult.contains("table_b"), "SHOW TABLES result should still contain 'table_b'");
        assertTrue(finalShowResult.contains("1 rows returned"), "Should indicate 1 row was returned");
    }

    @Test
    void testAggregationAndGroupByFlow() {
        System.out.println("\n--- Test: Aggregation and Group By ---");
        queryProcessor.execute("CREATE TABLE sales (product VARCHAR, region VARCHAR, amount INT);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('A', 'East', 100);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('B', 'West', 250);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('A', 'East', 150);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('A', 'West', 200);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('B', 'East', 300);");
        queryProcessor.execute("INSERT INTO sales (product, region, amount) VALUES ('B', 'West', 50);");

        System.out.println("\n--- Simple Aggregation: Total sales amount ---");
        System.out.println("--- Expected: SUM(amount) = 1050 ---");
        queryProcessor.execute("SELECT SUM(amount) FROM sales;");

        System.out.println("\n--- Aggregation with GROUP BY: Total sales per region ---");
        System.out.println("--- Expected: East=550, West=500 ---");
        queryProcessor.execute("SELECT region, SUM(amount) FROM sales GROUP BY region;");

        System.out.println("\n--- Aggregation with GROUP BY: Count of sales per product ---");
        System.out.println("--- Expected: A=3, B=3 ---");
        queryProcessor.execute("SELECT product, COUNT(*) FROM sales GROUP BY product;");
    }
}