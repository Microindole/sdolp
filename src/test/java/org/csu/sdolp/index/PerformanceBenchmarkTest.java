package org.csu.sdolp.index;

import org.csu.sdolp.engine.QueryProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.io.File;
import java.io.IOException;

/**
 * 数据库核心功能性能基准测试.
 */
public class PerformanceBenchmarkTest {

    private final String TEST_DB_NAME = "benchmark_db";
    private QueryProcessor queryProcessor;
    private final int BATCH_SIZE = 10000;

    @BeforeEach
    void setUp() {
        deleteDirectory(new File("data/" + TEST_DB_NAME));
        queryProcessor = new QueryProcessor(TEST_DB_NAME);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        if(queryProcessor != null) queryProcessor.close();
        deleteDirectory(new File("data/" + TEST_DB_NAME));
    }

    @Test
    void benchmarkBatchInsert() {
        System.out.println("--- Benchmark: Batch Insert Performance ---");
        queryProcessor.execute("CREATE TABLE perf_test (id INT PRIMARY KEY, data VARCHAR);");

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < BATCH_SIZE; i++) {
            queryProcessor.execute(String.format("INSERT INTO perf_test (id, data) VALUES (%d, 'data-%d');", i, i));
        }
        long endTime = System.currentTimeMillis();
        
        double durationSeconds = (endTime - startTime) / 1000.0;
        double insertsPerSecond = BATCH_SIZE / durationSeconds;

        System.out.printf("  - Inserted %d records in %.2f seconds.\n", BATCH_SIZE, durationSeconds);
        System.out.printf("  - Average inserts per second: %.2f\n", insertsPerSecond);
    }

    @Test
    void benchmarkIndexVsScan() {
        System.out.println("--- Benchmark: Index Scan vs. Full Table Scan ---");
        queryProcessor.execute("CREATE TABLE scan_test (id INT, data VARCHAR);");
        // 插入大量数据
        for (int i = 0; i < BATCH_SIZE * 2; i++) {
            queryProcessor.execute(String.format("INSERT INTO scan_test (id, data) VALUES (%d, 'data-%d');", i, i));
        }

        // 全表扫描
        long scanStartTime = System.nanoTime();
        queryProcessor.execute("SELECT * FROM scan_test WHERE data = 'data-15000';");
        long scanEndTime = System.nanoTime();
        System.out.printf("  - Full Table Scan took: %.2f ms\n", (scanEndTime - scanStartTime) / 1_000_000.0);
        
        // 创建索引
        System.out.println("  - Creating index on 'id' column...");
        queryProcessor.execute("CREATE INDEX idx_id ON scan_test (id);");

        // 索引扫描
        long indexStartTime = System.nanoTime();
        queryProcessor.execute("SELECT * FROM scan_test WHERE id = 15000;");
        long indexEndTime = System.nanoTime();
        System.out.printf("  - Index Scan took: %.2f ms\n", (indexEndTime - indexStartTime) / 1_000_000.0);
    }

    private void deleteDirectory(File directory) { /* ... */ }
}