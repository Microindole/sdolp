package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 专门用于量化验证谓词下推（Predicate Pushdown）对查询性能提升效果的集成测试。
 */
public class PredicatePushdownPerformanceTest {

    private final String TEST_DB_NAME = "pushdown_perf_test_db";
    private QueryProcessor queryProcessor;

    // --- 核心修复点：降低数据量以避免测试程序内存溢出 ---
    private static final int DATA_VOLUME = 10000; // 从 20000 降低到 5000
    private static final int TARGET_KEY = 9999;  // 相应地调整目标键

    @BeforeEach
    void setUp() {
        // --- 准备一个纯净的测试环境 ---
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);

        // --- 步骤 1: 插入大量数据 ---
        System.out.println("\n[步骤 1] 正在创建表并插入 " + DATA_VOLUME + " 条记录...");
        long startTime = System.currentTimeMillis();
        queryProcessor.execute("CREATE TABLE perf_table (id INT, name VARCHAR, value INT);");
        // 使用简单的循环插入数据
        for (int i = 1; i <= DATA_VOLUME; i++) {
            queryProcessor.execute(String.format("INSERT INTO perf_table (id, name, value) VALUES (%d, 'name_%d', %d);", i, i, i * 10));
        }
        long duration = System.currentTimeMillis() - startTime;
        System.out.printf("  - 数据插入完毕，耗时: %d 毫秒%n", duration);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testPredicatePushdownPerformanceGain() {
        System.out.println("--- [性能测试] 谓词下推加速效果验证 ---");

        // --- 步骤 2: 模拟无谓词下推（全量数据拉取） ---
        System.out.println("\n[步骤 2] 正在执行全表扫描查询 (无 WHERE 子句)...");
        String fullScanSql = "SELECT * FROM perf_table;";
        long startTimeWithoutPushdown = System.currentTimeMillis();
        String resultWithoutPushdown = queryProcessor.executeAndGetResult(fullScanSql);
        long durationWithoutPushdown = System.currentTimeMillis() - startTimeWithoutPushdown;
        System.out.printf("  - 全表扫描查询完成，耗时: %d 毫秒%n", durationWithoutPushdown);
        assertTrue(resultWithoutPushdown.contains(DATA_VOLUME + " rows returned"), "全表扫描应返回所有行。");

        // --- 步骤 3: 执行有谓词下推的选择性查询 ---
        System.out.println("\n[步骤 3] 正在执行应用了谓词下推的选择性查询...");
        String selectiveScanSql = "SELECT * FROM perf_table WHERE id = " + TARGET_KEY + ";";
        long startTimeWithPushdown = System.currentTimeMillis();
        String resultWithPushdown = queryProcessor.executeAndGetResult(selectiveScanSql);
        long durationWithPushdown = System.currentTimeMillis() - startTimeWithPushdown;
        System.out.printf("  - 选择性查询完成，耗时: %d 毫秒%n", durationWithPushdown);
        assertTrue(resultWithPushdown.contains("1 rows returned"), "选择性查询应只返回一行。");
        assertTrue(resultWithPushdown.contains("name_" + TARGET_KEY), "返回的应该是正确的数据。");

        // --- 步骤 4: 断言和验证 ---
        System.out.println("\n[步骤 4] 性能对比与最终验证...");
        System.out.printf("  - 性能提升倍数: %.2f 倍%n", (double) durationWithoutPushdown / durationWithPushdown);

        // 核心断言：带有谓词下推的查询必须比全量拉取快得多。
        assertTrue(durationWithPushdown < durationWithoutPushdown / 4,
                String.format("谓词下推带来的性能提升未达到预期！无下推耗时: %d ms, 有下推耗时: %d ms",
                        durationWithoutPushdown, durationWithPushdown));
        System.out.println("\n[测试通过] 谓词下推显著提升了查询性能，验证成功！");
    }

    /**
     * 辅助方法：用于递归删除目录。
     */
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