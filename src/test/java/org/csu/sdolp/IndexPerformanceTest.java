// src/test/java/org/csu/sdolp/index/IndexPerformanceTest.java
package org.csu.sdolp;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 专门用于量化验证 B+树索引对查询性能提升效果的集成测试。
 * [调试增强版]
 */
public class IndexPerformanceTest {

    private final String TEST_DB_NAME = "index_perf_test_db";
    private QueryProcessor queryProcessor;

    private static final int DATA_VOLUME = 10000;
    private static final int TARGET_KEY = 6500;

    @BeforeEach
    void setUp() {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (queryProcessor != null) {
            queryProcessor.close();
        }
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void testIndexPerformanceGain() {
        System.out.println("--- [性能测试] B+树索引加速效果验证 ---");

        // 步骤 1: 插入大量数据
        System.out.println("\n[步骤 1] 正在创建表并插入 " + DATA_VOLUME + " 条记录...");
        queryProcessor.execute("CREATE TABLE perf_test (id INT, name VARCHAR);");
        for (int i = 1; i <= DATA_VOLUME; i++) {
            queryProcessor.execute(String.format("INSERT INTO perf_test (id, name) VALUES (%d, 'user%d');", i, i));
        }
        System.out.println("  - 数据插入完毕。");

        // 步骤 2: 无索引查询
        System.out.println("\n[步骤 2] 正在执行全表扫描查询 (无索引)...");
        String querySql = "SELECT * FROM perf_test WHERE id = " + TARGET_KEY + ";";
        long startTimeWithoutIndex = System.currentTimeMillis();
        String resultWithoutIndex = queryProcessor.executeAndGetResult(querySql);
        long durationWithoutIndex = System.currentTimeMillis() - startTimeWithoutIndex;
        System.out.printf("  - 全表扫描查询完成，耗时: %d 毫秒%n", durationWithoutIndex);
        assertTrue(resultWithoutIndex.contains("user" + TARGET_KEY), "全表扫描未能找到正确的数据。");

        // 步骤 3: 创建索引
        System.out.println("\n[步骤 3] 正在为 'id' 列创建 B+树索引...");
        queryProcessor.execute("CREATE INDEX idx_perf_test_id ON perf_test (id);");
        System.out.println("  - 索引创建指令执行完毕。");

        // --- 关键的调试步骤 ---
        System.out.println("\n--- [调试日志] 查询 Catalog 元数据表，检查持久化的 Root Page ID ---");
        String catalogQuery = "SELECT root_page_id FROM _catalog_indexes WHERE index_name = 'idx_perf_test_id';";
        String catalogResult = queryProcessor.executeAndGetResult(catalogQuery);
        System.out.println(catalogResult);
        // --- 调试结束 ---

        // 步骤 4: 有索引的情况下再次查询
        System.out.println("\n[步骤 4] 正在执行索引扫描查询 (有索引)...");
        long startTimeWithIndex = System.currentTimeMillis();
        String resultWithIndex = queryProcessor.executeAndGetResult(querySql);
        long durationWithIndex = System.currentTimeMillis() - startTimeWithIndex;

        System.out.println("\n--- [调试日志] 索引扫描的完整返回结果 ---");
        System.out.println(resultWithIndex);
        System.out.println("--- 调试结束 ---");

        System.out.printf("  - 索引扫描查询完成，耗时: %d 毫秒%n", durationWithIndex);

        // 步骤 5: 断言和验证
        assertTrue(resultWithIndex.contains("user" + TARGET_KEY), "索引扫描未能找到正确的数据。");
        System.out.println("\n[步骤 5] 性能对比与最终验证...");
        System.out.printf("  - 性能提升倍数: %.2f 倍%n", (double) durationWithoutIndex / durationWithIndex);
        assertTrue(durationWithIndex < durationWithoutIndex / 5,
                String.format("索引性能提升未达到预期！无索引耗时: %d ms, 有索引耗时: %d ms",
                        durationWithoutIndex, durationWithIndex));
        System.out.println("\n[测试通过] B+树索引显著提升了查询性能，验证成功！");
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