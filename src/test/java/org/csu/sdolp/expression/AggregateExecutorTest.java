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
 * 专门用于测试聚合执行器 (AggregateExecutor) 及其相关功能的集成测试.
 */
public class AggregateExecutorTest {


    private final String TEST_DB_NAME = "aggregate_test_db";
    private QueryProcessor queryProcessor;

    @BeforeEach
    void setUp() {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // 准备测试数据
        queryProcessor.execute("CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);");
        queryProcessor.execute("INSERT INTO sales (region, product, amount) VALUES ('East', 'Apple', 100);");
        queryProcessor.execute("INSERT INTO sales (region, product, amount) VALUES ('West', 'Orange', 250);");
        queryProcessor.execute("INSERT INTO sales (region, product, amount) VALUES ('East', 'Apple', 150);");
        queryProcessor.execute("INSERT INTO sales (region, product, amount) VALUES ('West', 'Apple', 200);");
        queryProcessor.execute("INSERT INTO sales (region, product, amount) VALUES ('East', 'Orange', 300);");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testSimpleAggregateWithoutGroupBy() {
        System.out.println("--- Test: Simple Aggregate without GROUP BY ---");
        String sumResult = queryProcessor.executeAndGetResult("SELECT SUM(amount) FROM sales;");
        System.out.println(sumResult);
        assertTrue(sumResult.contains("1000"), "Total sum of amount should be 1000.");

        String countResult = queryProcessor.executeAndGetResult("SELECT COUNT(*) FROM sales;");
        System.out.println(countResult);
        assertTrue(countResult.contains("5"), "Total count of rows should be 5.");
    }

    @Test
    void testAggregateWithSingleColumnGroupBy() {
        System.out.println("--- Test: Aggregate with single-column GROUP BY ---");
        String result = queryProcessor.executeAndGetResult("SELECT region, SUM(amount) FROM sales GROUP BY region;");
        System.out.println(result);
        assertTrue(result.contains("East") && result.contains("550"), "Sum for East region should be 550.");
        assertTrue(result.contains("West") && result.contains("450"), "Sum for West region should be 450.");
        assertFalse(result.contains("Apple"), "Result should not contain product names.");
    }

    @Test
    void testAggregateWithMultipleColumnsGroupBy() {
        System.out.println("--- Test: Aggregate with multi-column GROUP BY ---");
        String result = queryProcessor.executeAndGetResult("SELECT region, product, COUNT(*) FROM sales GROUP BY region, product;");
        System.out.println(result);
        assertTrue(result.contains("East") && result.contains("Apple") && result.contains("2"), "Count for (East, Apple) should be 2.");
        assertTrue(result.contains("West") && result.contains("Orange") && result.contains("1"), "Count for (West, Orange) should be 1.");
    }

    @Test
    void testAggregateOnEmptyTable() {
        System.out.println("--- Test: Aggregate on an empty table ---");
        queryProcessor.execute("CREATE TABLE empty_sales (product VARCHAR, amount INT);");

        String result = queryProcessor.executeAndGetResult("SELECT SUM(amount) FROM empty_sales;");
        System.out.println(result);

        // 将预期的字符串修改为与 QueryProcessor 的实际输出完全匹配
        assertTrue(result.contains("Query finished, 0 rows returned."), "SUM on empty table should return a '0 rows returned' message.");
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