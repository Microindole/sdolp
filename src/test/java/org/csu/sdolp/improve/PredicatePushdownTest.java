package org.csu.sdolp.improve;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于测试谓词下推 (Predicate Pushdown) 功能的集成测试.
 * <p>
 * 该测试验证 WHERE 子句中的过滤条件是否被成功下推到扫描层执行，
 * 从而提高 SELECT, UPDATE, DELETE 等操作的效率和正确性。
 */
public class PredicatePushdownTest {

    private final String TEST_DB_NAME = "predicate_pushdown_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        // --- 在每个测试开始前，清理并重建一个干净的数据库环境 ---
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // --- 准备测试数据 ---
        // 创建一个包含不同数据类型的表
        queryProcessor.execute("CREATE TABLE products (id INT, name VARCHAR, category VARCHAR, stock INT);");
        // 插入多条记录，以便进行丰富的查询测试
        queryProcessor.execute("INSERT INTO products (id, name, category, stock) VALUES (1, 'Apple', 'Fruit', 100);");
        queryProcessor.execute("INSERT INTO products (id, name, category, stock) VALUES (2, 'Milk', 'Dairy', 50);");
        queryProcessor.execute("INSERT INTO products (id, name, category, stock) VALUES (3, 'Bread', 'Bakery', 80);");
        queryProcessor.execute("INSERT INTO products (id, name, category, stock) VALUES (4, 'Orange', 'Fruit', 120);");
        queryProcessor.execute("INSERT INTO products (id, name, category, stock) VALUES (5, 'Cheese', 'Dairy', 30);");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        // --- 测试结束后清理环境 ---
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    /**
     * 测试场景 1: 基本的 SELECT 查询与谓词下推
     * 验证 WHERE 子句是否能正确过滤数据。
     */
    @Test
    void testSelectWithPredicatePushdown() {
        System.out.println("--- Test: Basic SELECT with Predicate Pushdown ---");
        String sql = "SELECT name, stock FROM products WHERE category = 'Dairy';";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("Query: " + sql);
        System.out.println("Result:\n" + result);

        // 验证结果是否正确
        assertTrue(result.contains("Milk"), "结果应包含 'Milk'");
        assertTrue(result.contains("Cheese"), "结果应包含 'Cheese'");
        assertFalse(result.contains("Apple"), "结果不应包含 'Apple'");
        assertTrue(result.contains("2 rows returned"), "应返回2行数据");
    }

    /**
     * 测试场景 2: SELECT 查询，但没有匹配的行
     * 验证谓词下推是否能正确处理没有结果的情况。
     */
    @Test
    void testSelectWithNoMatchingRows() {
        System.out.println("--- Test: SELECT with No Matching Rows ---");
        String sql = "SELECT * FROM products WHERE stock < 20;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("Query: " + sql);
        System.out.println("Result:\n" + result);

        assertTrue(result.contains("0 rows returned"), "查询没有匹配项时，应返回0行");
    }

    /**
     * 测试场景 3: 没有任何 WHERE 子句的 SELECT
     * 验证在没有谓词需要下推时，系统是否能正常执行全表扫描。
     */
    @Test
    void testSelectWithoutPredicate() {
        System.out.println("--- Test: SELECT without any Predicate ---");
        String sql = "SELECT * FROM products;";
        String result = queryProcessor.executeAndGetResult(sql);

        System.out.println("Query: " + sql);
        System.out.println("Result:\n" + result);

        assertTrue(result.contains("5 rows returned"), "没有 WHERE 子句时，应返回所有5行数据");
    }


    /**
     * 测试场景 4: UPDATE 操作与谓词下推
     * 验证 WHERE 子句是否能定位到正确的行进行更新。
     */
    @Test
    void testUpdateWithPredicatePushdown() {
        System.out.println("--- Test: UPDATE with Predicate Pushdown ---");
        String updateSql = "UPDATE products SET stock = 75 WHERE name = 'Bread';";
        String updateResult = queryProcessor.executeAndGetResult(updateSql);

        System.out.println("Update Query: " + updateSql);
        // 理想情况下，UPDATE 会返回影响的行数，但当前实现返回 "Query OK."
        // 我们通过后续的 SELECT 来验证其正确性

        String verifySql = "SELECT stock FROM products WHERE id = 3;";
        String verifyResult = queryProcessor.executeAndGetResult(verifySql);
        System.out.println("Verification Query: " + verifySql);
        System.out.println("Result:\n" + verifyResult);

        assertTrue(verifyResult.contains("75"), "更新操作后，'Bread' 的库存应该是 75");
    }

    /**
     * 测试场景 5: DELETE 操作与谓词下推
     * 验证 WHERE 子句是否能定位到正确的行进行删除。
     */
    @Test
    void testDeleteWithPredicatePushdown() {
        System.out.println("--- Test: DELETE with Predicate Pushdown ---");
        String deleteSql = "DELETE FROM products WHERE category = 'Fruit';";
        queryProcessor.executeAndGetResult(deleteSql);
        System.out.println("Delete Query: " + deleteSql);

        // 验证删除后，剩余的行数是否正确
        String verifySql = "SELECT * FROM products;";
        String verifyResult = queryProcessor.executeAndGetResult(verifySql);
        System.out.println("Verification Query: " + verifySql);
        System.out.println("Result:\n" + verifyResult);

        assertFalse(verifyResult.contains("Apple"), "删除'Fruit'类别后，'Apple'不应存在");
        assertFalse(verifyResult.contains("Orange"), "删除'Fruit'类别后，'Orange'不应存在");
        assertTrue(verifyResult.contains("3 rows returned"), "删除2条记录后，应还剩3行数据");
    }

    /**
     * 辅助方法：用于递归删除目录，确保测试环境纯净。
     */
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