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