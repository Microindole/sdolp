package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

/**
 * 端到端的系统集成测试.
 * 测试从SQL字符串到执行结果的全过程，并验证数据持久性。
 */
public class SystemIntegrationTest {

    private final String TEST_DB_FILE = "system_integration_test.db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // 每次测试前删除旧的数据库文件
        new File(TEST_DB_FILE).delete();
        queryProcessor = new QueryProcessor(TEST_DB_FILE);
    }

    @AfterEach
    void tearDown() throws IOException {
        queryProcessor.close();
        new File(TEST_DB_FILE).delete();
    }

    @Test
    void testCreateInsertSelectFlow() {
        System.out.println("--- Test: Create, Insert, and Select ---");
        queryProcessor.execute("CREATE TABLE users (id INT, name VARCHAR, age INT);");
        queryProcessor.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 20);");
        queryProcessor.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);");
        queryProcessor.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 20);");
        
        System.out.println("\n--- Selecting all users ---");
        queryProcessor.execute("SELECT * FROM users;");
        
        System.out.println("\n--- Selecting users with age = 20 ---");
        queryProcessor.execute("SELECT id, name FROM users WHERE age = 20;");
    }

    @Test
    void testUpdateDeleteFlow() {
        System.out.println("--- Test: Update and Delete ---");
        queryProcessor.execute("CREATE TABLE products (pid INT, price INT);");
        queryProcessor.execute("INSERT INTO products (pid, price) VALUES (101, 50);");
        queryProcessor.execute("INSERT INTO products (pid, price) VALUES (102, 100);");
        queryProcessor.execute("INSERT INTO products (pid, price) VALUES (103, 150);");

        System.out.println("\n--- Products before update ---");
        queryProcessor.execute("SELECT * FROM products;");

        System.out.println("\n--- Updating price for pid = 102 ---");
        queryProcessor.execute("UPDATE products SET price = 120 WHERE pid = 102;");

        System.out.println("\n--- Products after update ---");
        queryProcessor.execute("SELECT * FROM products;");

        System.out.println("\n--- Deleting products with price < 100 ---");
        queryProcessor.execute("DELETE FROM products WHERE price < 100;");
        
        System.out.println("\n--- Products after delete ---");
        queryProcessor.execute("SELECT * FROM products;");
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
    void testDataPersistence() throws IOException {
        System.out.println("--- Test: Data Persistence ---");
        queryProcessor.execute("CREATE TABLE persistent_table (id INT);");
        queryProcessor.execute("INSERT INTO persistent_table (id) VALUES (999);");

        System.out.println("\n--- Closing database... ---");
        queryProcessor.close();

        System.out.println("\n--- Reopening database... ---");
        queryProcessor = new QueryProcessor(TEST_DB_FILE);

        System.out.println("\n--- Selecting from table after reopening ---");
        // 这条查询将验证CREATE TABLE和INSERT操作是否都已持久化
        queryProcessor.execute("SELECT * FROM persistent_table WHERE id = 999;");
    }
}